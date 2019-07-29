# echo_client 
import sys
import trio
import struct
import logging
import DTC_Files.DTCProtocol_pb2 as Dtc
import termios, tty
import ssl
import subprocess
import datetime

logMsg = logging.getLogger('johndtc')
logPrc = logging.getLogger('johnprice')
logStdout = logging.getLogger('johnstdout')

logMsg.setLevel(logging.DEBUG)
logPrc.setLevel(logging.INFO)
logStdout.setLevel(logging.DEBUG)

logPrc_handler = logging.FileHandler('logPrc-cl.txt')
logMsg_handler = logging.FileHandler('logMsg-hist.txt')
stdout_handler = logging.StreamHandler()

fmtMsg = logging.Formatter("%(asctime)s : %(levelname)s : %(funcName)s : %(message)s")
fmtPrc = logging.Formatter("%(asctime)s,%(message)s")
fmtStdout = logging.Formatter("%(asctime)s : %(levelname)s : %(funcName)s : %(message)s")

logPrc_handler.setFormatter(fmtPrc)
logMsg_handler.setFormatter(fmtMsg)
stdout_handler.setFormatter(fmtStdout)

logMsg.addHandler(logMsg_handler)
logStdout.addHandler(stdout_handler)
logPrc.addHandler(logPrc_handler)

# Mapping message type to DTC message object and human readable name
DTC_MTYPE_MAP = {
 1: ['LOGON_REQUEST', 0, 1, Dtc.LogonRequest],
 2: ['LOGON_RESPONSE', 1, 2, Dtc.LogonResponse],
 3: ['HEARTBEAT', 2, 3, Dtc.Heartbeat],
 5: ['LOGOFF', 3, 5, Dtc.Logoff],
 6: ['ENCODING_REQUEST', 4, 6, Dtc.EncodingRequest],
 7: ['ENCODING_RESPONSE', 5, 7, Dtc.EncodingResponse],
 101: ['MARKET_DATA_REQUEST', 6, 101, Dtc.MarketDataRequest],
 103: ['MARKET_DATA_REJECT', 7, 103, Dtc.MarketDataReject],
 104: ['MARKET_DATA_SNAPSHOT', 8, 104, Dtc.MarketDataSnapshot],
 107: ['MARKET_DATA_UPDATE_TRADE', 10, 107, Dtc.MarketDataUpdateTrade],
 112: ['MARKET_DATA_UPDATE_TRADE_COMPACT', 11, 112, Dtc.MarketDataUpdateTradeCompact],
 134: ['MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT', 13, 134, Dtc.MarketDataUpdateLastTradeSnapshot],
 108: ['MARKET_DATA_UPDATE_BID_ASK', 14, 108, Dtc.MarketDataUpdateBidAsk],
 117: ['MARKET_DATA_UPDATE_BID_ASK_COMPACT', 15, 117, Dtc.MarketDataUpdateBidAskCompact],
 120: ['MARKET_DATA_UPDATE_SESSION_OPEN', 17, 120, Dtc.MarketDataUpdateSessionOpen],
 114: ['MARKET_DATA_UPDATE_SESSION_HIGH', 19, 114, Dtc.MarketDataUpdateSessionHigh],
 115: ['MARKET_DATA_UPDATE_SESSION_LOW', 21, 115, Dtc.MarketDataUpdateSessionLow],
 113: ['MARKET_DATA_UPDATE_SESSION_VOLUME', 23, 113, Dtc.MarketDataUpdateSessionVolume],
 124: ['MARKET_DATA_UPDATE_OPEN_INTEREST', 24, 124, Dtc.MarketDataUpdateOpenInterest],
 119: ['MARKET_DATA_UPDATE_SESSION_SETTLEMENT', 25, 119, Dtc.MarketDataUpdateSessionSettlement],
 135: ['MARKET_DATA_UPDATE_SESSION_NUM_TRADES', 27, 135, Dtc.MarketDataUpdateSessionNumTrades],
 136: ['MARKET_DATA_UPDATE_TRADING_SESSION_DATE', 28, 136, Dtc.MarketDataUpdateTradingSessionDate],
 102: ['MARKET_DEPTH_REQUEST', 29, 102, Dtc.MarketDepthRequest],
 121: ['MARKET_DEPTH_REJECT', 30, 121, Dtc.MarketDepthReject],
 122: ['MARKET_DEPTH_SNAPSHOT_LEVEL', 31, 122, Dtc.MarketDepthSnapshotLevel],
 106: ['MARKET_DEPTH_UPDATE_LEVEL', 33, 106, Dtc.MarketDepthUpdateLevel],
 118: ['MARKET_DEPTH_UPDATE_LEVEL_COMPACT', 34, 118, Dtc.MarketDepthUpdateLevelCompact],
 123: ['MARKET_DEPTH_FULL_UPDATE_10', 36, 123, Dtc.MarketDepthFullUpdate10],
 105: ['MARKET_DEPTH_FULL_UPDATE_20', 37, 105, Dtc.MarketDepthFullUpdate20],
 100: ['MARKET_DATA_FEED_STATUS', 38, 100, Dtc.MarketDataFeedStatus],
 116: ['MARKET_DATA_FEED_SYMBOL_STATUS', 39, 116, Dtc.MarketDataFeedSymbolStatus],
 208: ['SUBMIT_NEW_SINGLE_ORDER', 40, 208, Dtc.SubmitNewSingleOrder],
 203: ['CANCEL_ORDER', 44, 203, Dtc.CancelOrder],
 204: ['CANCEL_REPLACE_ORDER', 45, 204, Dtc.CancelReplaceOrder],
 300: ['OPEN_ORDERS_REQUEST', 47, 300, Dtc.OpenOrdersRequest],
 302: ['OPEN_ORDERS_REJECT', 48, 302, Dtc.OpenOrdersReject],
 301: ['ORDER_UPDATE', 49, 301, Dtc.OrderUpdate],
 303: ['HISTORICAL_ORDER_FILLS_REQUEST', 50, 303, Dtc.HistoricalOrderFillsRequest],
 304: ['HISTORICAL_ORDER_FILL_RESPONSE', 51, 304, Dtc.HistoricalOrderFillResponse],
 308: ['HISTORICAL_ORDER_FILLS_REJECT', 52, 308, Dtc.HistoricalOrderFillsReject],
 305: ['CURRENT_POSITIONS_REQUEST', 53, 305, Dtc.CurrentPositionsRequest],
 307: ['CURRENT_POSITIONS_REJECT', 54, 307, Dtc.CurrentPositionsReject],
 306: ['POSITION_UPDATE', 55, 306, Dtc.PositionUpdate],
 400: ['TRADE_ACCOUNTS_REQUEST', 56, 400, Dtc.TradeAccountsRequest],
 401: ['TRADE_ACCOUNT_RESPONSE', 57, 401, Dtc.TradeAccountResponse],
 500: ['EXCHANGE_LIST_REQUEST', 58, 500, Dtc.ExchangeListRequest],
 501: ['EXCHANGE_LIST_RESPONSE', 59, 501, Dtc.ExchangeListResponse],
 502: ['SYMBOLS_FOR_EXCHANGE_REQUEST', 60, 502, Dtc.SymbolsForExchangeRequest],
 503: ['UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST', 61, 503, Dtc.UnderlyingSymbolsForExchangeRequest],
 504: ['SYMBOLS_FOR_UNDERLYING_REQUEST', 62, 504, Dtc.SymbolsForUnderlyingRequest],
 506: ['SECURITY_DEFINITION_FOR_SYMBOL_REQUEST', 63, 506, Dtc.SecurityDefinitionForSymbolRequest],
 507: ['SECURITY_DEFINITION_RESPONSE', 64, 507, Dtc.SecurityDefinitionResponse],
 508: ['SYMBOL_SEARCH_REQUEST', 65, 508, Dtc.SymbolSearchRequest],
 509: ['SECURITY_DEFINITION_REJECT', 66, 509, Dtc.SecurityDefinitionReject],
 601: ['ACCOUNT_BALANCE_REQUEST', 67, 601, Dtc.AccountBalanceRequest],
 602: ['ACCOUNT_BALANCE_REJECT', 68, 602, Dtc.AccountBalanceReject],
 600: ['ACCOUNT_BALANCE_UPDATE', 69, 600, Dtc.AccountBalanceUpdate],
 700: ['USER_MESSAGE', 70, 700, Dtc.UserMessage],
 701: ['GENERAL_LOG_MESSAGE', 71, 701, Dtc.GeneralLogMessage],
 800: ['HISTORICAL_PRICE_DATA_REQUEST', 72, 800, Dtc.HistoricalPriceDataRequest],
 801: ['HISTORICAL_PRICE_DATA_RESPONSE_HEADER', 73, 801, Dtc.HistoricalPriceDataResponseHeader],
 802: ['HISTORICAL_PRICE_DATA_REJECT', 74, 802, Dtc.HistoricalPriceDataReject],
 803: ['HISTORICAL_PRICE_DATA_RECORD_RESPONSE', 75, 803, Dtc.HistoricalPriceDataRecordResponse],
 804: ['HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE', 76, 804, Dtc.HistoricalPriceDataTickRecordResponse],
}

TYPE_TO_STRUCT_FORMAT = {
    1: "i",  # CPPTYPE_INT32
    2: "l",  # CPPTYPE_INT64
    3: "I",  # CPPTYPE_UINT32
    4: "L",  # CPPTYPE_UINT64
    5: "d",  # CPPTYPE_DOUBLE
    6: "f",  # CPPTYPE_FLOAT
    7: "?",  # CPPTYPE_BOOL
    8: "ENUM",  # CPPTYPE_ENUM; Special handling required
    9: "s",  # CPPTYPE_STRING
    10: "s"  # CPPTYPE_MESSAGE; TODO: this is an assumption, needs review
}

def create_enc_req(encoding):
    enc_req = Dtc.EncodingRequest()
    enc_req.Encoding = encoding
    enc_req.ProtocolType = "DTC"
    enc_req.ProtocolVersion = Dtc.CURRENT_VERSION
    return enc_req

def create_logon_req(heartbeat_interval):
    logon_req = Dtc.LogonRequest()
    logon_req.ProtocolVersion = Dtc.CURRENT_VERSION
    # logon_req.Username = "wat"
    # logon_req.Password = "wat"
    logon_req.GeneralTextData = "John's Test"
    logon_req.HeartbeatIntervalInSeconds = heartbeat_interval
    logon_req.ClientName = "John_Tester"
    return logon_req

def create_mktdat_req(symbolID, symbol, exchange):
    data_req = Dtc.MarketDataRequest()
    data_req.RequestAction = Dtc.SUBSCRIBE
    data_req.SymbolID = symbolID  # Note: Make sure this is unique when requesting multiple symbols
    data_req.Symbol = symbol
    data_req.Exchange = exchange
    return data_req

def create_histprcdat_req(requestID, symbol, exchange, record_interval, start=0, end=0, max=0):
    data_req = Dtc.HistoricalPriceDataRequest()
    data_req.RequestID = requestID  # Note: Make sure this is unique when requesting multiple symbols
    data_req.Symbol = symbol
    data_req.Exchange = exchange
    data_req.RecordInterval = record_interval
    data_req.StartDateTime = start
    data_req.EndDateTime = end
    data_req.MaxDaysToReturn = max
    return data_req

async def heartbeater(client_stream, heartbeat_interval):
    logMsg.debug("Heartbeater started")
    hrtbt = Dtc.Heartbeat()
    while True:
        logStdout.info('Sending Heartbeat')
        await send_message(hrtbt, Dtc.HEARTBEAT,client_stream)
        await trio.sleep(heartbeat_interval)

def chekker2(m):
    pktp = tuple(getattr(m,f.name) for f in m.DESCRIPTOR.fields)
    return pktp

async def send_message(m, m_type, client_stream):
    total_len = 4 + m.ByteSize()
    header = struct.pack('HH', total_len, m_type)
    binary_message = m.SerializeToString()
    logStdout.info('sending: %s',m_type)
    await client_stream.send_all(header + binary_message)
    logStdout.info("Sent : %s", m_type)
        
async def mkt_updater(client_stream, gp):
    logMsg.debug("mkt_updater started")
    logStdout.info("mkt_updater started")
    buflen = 200
    buf = []
    while True:
        mtype, data = await get_response(client_stream)
        # logMsg.info(f"mkt_updater: got data {data}")
        if mtype == 'MARKET_DATA_UPDATE_TRADE_COMPACT':
            data = chekker2(data)
            # logPrc.info("%s, %s",mtype,data)
            t = data[-3] #datetime.datetime.fromtimestamp(data[-3])
            u = data[0]
            buf.append( (float(t), float(u)) )
            buf = buf[-buflen:]            # drop oldest data points,
                                        # retaining at most buflen points
            mx = buf[-1][0]                # most recent time stamp
            await gp.stdin.send_all( b"set xrange [%f:%f]\n" % ( mx-50, mx+1 ) )
            await gp.stdin.send_all( b"plot '-' u 1:2 w lp\n" )
            for d in buf:
                await gp.stdin.send_all( b"%f %f\n" % d )
            await gp.stdin.send_all( b"e\n" )
            await trio.sleep(0.001)
        # if not data:
        #     logMsg.info("mkt_updater: connection closed")
        #     sys.exit()

async def hist_downloader(client_stream):
    logMsg.debug("hist_downloader started")
    logStdout.info("hist_downloader started")
    while True:
        # header = client_stream.recv(4)
        # m_size = struct.unpack_from('<H', header[:2])[0]
        # m_type = struct.unpack_from('<H', header[2:4])[0]
        # m_body = client_stream.recv(m_size - 4)
        
        # m_type = DTC_MTYPE_MAP[m_type]
        # print('HEEYYY: M-TYPE is: ', m_type)
        # print('M-BODY :', m_body)
        # m_resp = m_type[3]()
        # m_resp.ParseFromString(m_body)
        # logMsg.info(f"mkt_updater: got data {data}")
        m_type, data = await get_response(client_stream)
        data = chekker2(data)
        # logPrc.info("%s, %s",mtype,data)
        logMsg.debug("%s, %s",m_type,data)
        logStdout.debug("%s, %s",m_type,data)
        await trio.sleep(0.001)

async def get_response(client_stream):
    header = await client_stream.receive_some(4)
    m_size = struct.unpack_from('<H', header[:2])[0] # the size of message
    m_type = struct.unpack_from('<H', header[2:4])[0] # the type of the message
    m_body = await client_stream.receive_some(m_size - 4)
    m_type = DTC_MTYPE_MAP[m_type]
    m_resp = m_type[3]()
    m_resp.ParseFromString(m_body)

    # logMsg.debug(m_type[0])
    logStdout.debug(m_type[0])
    # logMsg.debug("m_size: %s ", m_size)
    # logMsg.debug("m_body: %s ", m_body)
    # logMsg.debug("type m_resp: %s ", m_type[3])
    # logMsg.debug("  CHEKKER: %s", chekker(m_resp))
    return m_type[0],m_resp

async def quit(sock):
    """ Gracefully logoff and close the connection """
    logMsg.info(" QUIT!!! : Gracefully logoff and close the connection ")
    logStdout.info(" QUIT!!! : Gracefully logoff and close the connection ")
    logoff = Dtc.Logoff()
    logoff.Reason = "Client quit"
    await send_message(logoff, Dtc.LOGOFF, sock)
    # Gracefully close the socket
    # sock.close()
    logMsg.info('--------------------------------------------------------------')
    logStdout.info('--------------------------------------------------------------')

# -- NEW
async def keyboard():
    """Return an iterator of characters from stdin."""
    stashed_term = termios.tcgetattr(sys.stdin)
    try:
        tty.setcbreak(sys.stdin, termios.TCSANOW)
        while True:
            yield await trio.run_sync_in_worker_thread(sys.stdin.read, 1,cancellable=True)
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSANOW, stashed_term)

async def trigger(event,scope, sock): #,gp):
    logMsg.info("  trigger waiting now...")
    logStdout.info("  trigger waiting now...")
    await event.wait()
    logMsg.info("  !!! Trigger event!!!")
    logStdout.info("  trigger event now...")
    # gp.kill()
    await quit(sock)
    scope.cancel()

async def parent(addr, encoding, heartbeat_interval):
    global event
    logMsg.info(f"parent: connecting to {addr[0]}:{addr[1]}")
    logStdout.info(f"parent: connecting to {addr[0]}:{addr[1]}")
    client_stream = await trio.open_tcp_stream(addr[0], addr[1])
    # client_stream = await trio.open_ssl_over_tcp_stream(addr[0], addr[1], 
    #     ssl_context=ssl.SSLContext())
    
    ## start gnuplot subprocess
    # buflen = 100                     # number of data points to buffer
    # gnuplot = "/usr/bin/gnuplot"

    # gnuplot = b"gnuplot"
    # gp = trio.Process(gnuplot, stdin=subprocess.PIPE)
    # await gp.stdin.send_all( b"set t qt\n" )
    
    async with client_stream:
        # encoding request
        logMsg.info("spawning encoding request ...")
        enc_req = create_enc_req(encoding) # construct encoding request
        await send_message(enc_req, Dtc.ENCODING_REQUEST,client_stream) # send encoding request
        response = await get_response(client_stream)
        logMsg.info("%s ", response)

        # logon request
        logMsg.info("parent: spawning logon request ...")
        logon_req = create_logon_req(heartbeat_interval)
        await send_message(logon_req, Dtc.LOGON_REQUEST, client_stream)
        response = await get_response(client_stream)
        logMsg.info("%s ", response)

        # Start nursery
        async with trio.open_nursery() as nursery:
            # trigger
            logMsg.info("spawning trigger...")
            nursery.start_soon(trigger, event, nursery.cancel_scope, client_stream)#, gp)
            logMsg.info("spawned trigger ...")

            hist_req = create_histprcdat_req(999,'F.US.ZFTU19', 'SGX', 0)
            await send_message(hist_req, Dtc.HISTORICAL_PRICE_DATA_REQUEST, client_stream)
            response = await get_response(client_stream)
            logMsg.debug("%s ", response)
            logStdout.debug("%s ", response)

            logMsg.info("parent: spawning hist_downloader ...")
            nursery.start_soon(hist_downloader, client_stream)

            # data_req = create_mktdat_req(88, 'F.US.CLEU19', 'GLOBEX')
            # await send_message(data_req, Dtc.MARKET_DATA_REQUEST, client_stream)
            # response = await get_response(client_stream)
            # logMsg.info("%s ", response)
            # logStdout.debug("%s ", response)

            # logMsg.info("parent: spawning heartbeater ...")
            # nursery.start_soon(heartbeater, client_stream, heartbeat_interval)

            # logMsg.info("parent: spawning mkt_updater ...")
            # nursery.start_soon(mkt_updater, client_stream, gp)

            # -- NEW
            async for key in keyboard():
                if key == 'q':
                    # nursery.cancel_scope.cancel()
                    event.set()

            logMsg.info("parent: waiting for tasks to finish...")
            logStdout.info("parent: waiting for tasks to finish...")
        # await quit(client_stream)
        logMsg.info("  Logoff Successful!!!")
        logStdout.info("  Logoff Successful!!!")
    logMsg.info("  Socket Closed!!!")
    logStdout.info("  Socket Closed!!!")


# Connection Params setup 
PORT = 11099
BUFSIZE = 1024
FLAG = 1
addr0 = ("127.0.0.1", 11099)

encoding = Dtc.PROTOCOL_BUFFERS
# encoding = Dtc.BINARY_ENCODING
heartbeat_interval = 10
event = trio.Event()
trio.run(parent, addr1_hist, encoding, heartbeat_interval)