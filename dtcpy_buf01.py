# echo_client 
import sys
import trio
import struct
import logging
import DTC_Files.DTCProtocol_pb2 as Dtc
import termios, tty
# import ssl
# import subprocess
# import datetime

import numpy as np

logStdout = logging.getLogger('johnstdout')
logStdout.setLevel(logging.DEBUG)
stdout_handler = logging.StreamHandler()
fmtStdout = logging.Formatter("%(asctime)s : %(levelname)s : %(funcName)s : %(message)s")
stdout_handler.setFormatter(fmtStdout)
logStdout.addHandler(stdout_handler)

DMAP = {
 1: ('LOGON_REQUEST', Dtc.LogonRequest),
 2: ('LOGON_RESPONSE', Dtc.LogonResponse),
 3: ('HEARTBEAT', Dtc.Heartbeat),
 5: ('LOGOFF', Dtc.Logoff),
 6: ('ENCODING_REQUEST', Dtc.EncodingRequest),
 7: ('ENCODING_RESPONSE', Dtc.EncodingResponse),
 101: ('MARKET_DATA_REQUEST', Dtc.MarketDataRequest),
 103: ('MARKET_DATA_REJECT', Dtc.MarketDataReject),
 104: ('MARKET_DATA_SNAPSHOT', Dtc.MarketDataSnapshot),
 107: ('MARKET_DATA_UPDATE_TRADE', Dtc.MarketDataUpdateTrade),
 112: ('MARKET_DATA_UPDATE_TRADE_COMPACT', Dtc.MarketDataUpdateTradeCompact),
 134: ('MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT', Dtc.MarketDataUpdateLastTradeSnapshot),
 108: ('MARKET_DATA_UPDATE_BID_ASK', Dtc.MarketDataUpdateBidAsk),
 117: ('MARKET_DATA_UPDATE_BID_ASK_COMPACT', Dtc.MarketDataUpdateBidAskCompact),
 120: ('MARKET_DATA_UPDATE_SESSION_OPEN', Dtc.MarketDataUpdateSessionOpen),
 114: ('MARKET_DATA_UPDATE_SESSION_HIGH', Dtc.MarketDataUpdateSessionHigh),
 115: ('MARKET_DATA_UPDATE_SESSION_LOW', Dtc.MarketDataUpdateSessionLow),
 113: ('MARKET_DATA_UPDATE_SESSION_VOLUME', Dtc.MarketDataUpdateSessionVolume),
 124: ('MARKET_DATA_UPDATE_OPEN_INTEREST', Dtc.MarketDataUpdateOpenInterest),
 119: ('MARKET_DATA_UPDATE_SESSION_SETTLEMENT', Dtc.MarketDataUpdateSessionSettlement),
 135: ('MARKET_DATA_UPDATE_SESSION_NUM_TRADES', Dtc.MarketDataUpdateSessionNumTrades),
 136: ('MARKET_DATA_UPDATE_TRADING_SESSION_DATE', Dtc.MarketDataUpdateTradingSessionDate),
 102: ('MARKET_DEPTH_REQUEST', Dtc.MarketDepthRequest),
 121: ('MARKET_DEPTH_REJECT', Dtc.MarketDepthReject),
 122: ('MARKET_DEPTH_SNAPSHOT_LEVEL', Dtc.MarketDepthSnapshotLevel),
 106: ('MARKET_DEPTH_UPDATE_LEVEL', Dtc.MarketDepthUpdateLevel),
 118: ('MARKET_DEPTH_UPDATE_LEVEL_COMPACT', Dtc.MarketDepthUpdateLevelCompact),
 123: ('MARKET_DEPTH_FULL_UPDATE_10', Dtc.MarketDepthFullUpdate10),
 105: ('MARKET_DEPTH_FULL_UPDATE_20', Dtc.MarketDepthFullUpdate20),
 100: ('MARKET_DATA_FEED_STATUS', Dtc.MarketDataFeedStatus),
 116: ('MARKET_DATA_FEED_SYMBOL_STATUS', Dtc.MarketDataFeedSymbolStatus),
 208: ('SUBMIT_NEW_SINGLE_ORDER', Dtc.SubmitNewSingleOrder),
 203: ('CANCEL_ORDER', Dtc.CancelOrder),
 204: ('CANCEL_REPLACE_ORDER', Dtc.CancelReplaceOrder),
 300: ('OPEN_ORDERS_REQUEST', Dtc.OpenOrdersRequest),
 302: ('OPEN_ORDERS_REJECT', Dtc.OpenOrdersReject),
 301: ('ORDER_UPDATE', Dtc.OrderUpdate),
 303: ('HISTORICAL_ORDER_FILLS_REQUEST', Dtc.HistoricalOrderFillsRequest),
 304: ('HISTORICAL_ORDER_FILL_RESPONSE', Dtc.HistoricalOrderFillResponse),
 308: ('HISTORICAL_ORDER_FILLS_REJECT', Dtc.HistoricalOrderFillsReject),
 305: ('CURRENT_POSITIONS_REQUEST', Dtc.CurrentPositionsRequest),
 307: ('CURRENT_POSITIONS_REJECT', Dtc.CurrentPositionsReject),
 306: ('POSITION_UPDATE', Dtc.PositionUpdate),
 400: ('TRADE_ACCOUNTS_REQUEST', Dtc.TradeAccountsRequest),
 401: ('TRADE_ACCOUNT_RESPONSE', Dtc.TradeAccountResponse),
 500: ('EXCHANGE_LIST_REQUEST', Dtc.ExchangeListRequest),
 501: ('EXCHANGE_LIST_RESPONSE', Dtc.ExchangeListResponse),
 502: ('SYMBOLS_FOR_EXCHANGE_REQUEST', Dtc.SymbolsForExchangeRequest),
 503: ('UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST', Dtc.UnderlyingSymbolsForExchangeRequest),
 504: ('SYMBOLS_FOR_UNDERLYING_REQUEST', Dtc.SymbolsForUnderlyingRequest),
 506: ('SECURITY_DEFINITION_FOR_SYMBOL_REQUEST', Dtc.SecurityDefinitionForSymbolRequest),
 507: ('SECURITY_DEFINITION_RESPONSE', Dtc.SecurityDefinitionResponse),
 508: ('SYMBOL_SEARCH_REQUEST', Dtc.SymbolSearchRequest),
 509: ('SECURITY_DEFINITION_REJECT', Dtc.SecurityDefinitionReject),
 601: ('ACCOUNT_BALANCE_REQUEST', Dtc.AccountBalanceRequest),
 602: ('ACCOUNT_BALANCE_REJECT', Dtc.AccountBalanceReject),
 600: ('ACCOUNT_BALANCE_UPDATE', Dtc.AccountBalanceUpdate),
 700: ('USER_MESSAGE', Dtc.UserMessage),
 701: ('GENERAL_LOG_MESSAGE', Dtc.GeneralLogMessage),
 800: ('HISTORICAL_PRICE_DATA_REQUEST', Dtc.HistoricalPriceDataRequest),
 801: ('HISTORICAL_PRICE_DATA_RESPONSE_HEADER', Dtc.HistoricalPriceDataResponseHeader),
 802: ('HISTORICAL_PRICE_DATA_REJECT', Dtc.HistoricalPriceDataReject),
 803: ('HISTORICAL_PRICE_DATA_RECORD_RESPONSE', Dtc.HistoricalPriceDataRecordResponse),
 804: ('HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE', Dtc.HistoricalPriceDataTickRecordResponse),
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

def create_loggen(logfile):
    """Create custom logger with own logfile for each DTC Request function"""
    logRec = logging.getLogger('General_Message')
    logRec.setLevel(logging.DEBUG)
    logRec_handler = logging.FileHandler(logfile+'_general')
    fmt = logging.Formatter("%(asctime)s, %(message)s")
    logRec_handler.setFormatter(fmt)
    logRec.addHandler(logRec_handler)
    return logRec

def create_mktdat_req(symbolID, symbol, exchange):
    data_req = Dtc.MarketDataRequest()
    data_req.RequestAction = Dtc.SUBSCRIBE
    data_req.SymbolID = symbolID  # Note: Make sure this is unique when requesting multiple symbols
    data_req.Symbol = symbol
    data_req.Exchange = exchange
    return data_req

async def heartbeater(client_stream, heartbeat_interval):
    # loggen.debug("Heartbeater started")
    hrtbt = Dtc.Heartbeat()
    while True:
        logStdout.info('Sending Heartbeat')
        await send_message(hrtbt, Dtc.HEARTBEAT,client_stream)
        await trio.sleep(heartbeat_interval)

async def send_message(m, m_type, client_stream):
    total_len = 4 + m.ByteSize()
    header = struct.pack('HH', total_len, m_type)
    binary_message = m.SerializeToString()
    logStdout.info('sending: %s',m_type)
    await client_stream.send_all(header + binary_message)
    logStdout.info("Sent : %s", m_type)
       
def chekker2(m):
    pktp = tuple(getattr(m,f.name) for f in m.DESCRIPTOR.fields)
    return pktp

async def process_row(receive_channel, prc_file):
    """receive price feed, process it and store in a np.array"""
    prc_list = []
    async for value in receive_channel:
        # differentiate between 112 and 117
        
        if value[0] == 117:
            m_resp = Dtc.MarketDataUpdateBidAskCompact()
            m_resp.ParseFromString(value[1])
            msg = np.array([np.uint32(value[0]), np.uint32(m_resp.DateTime), np.uint32(m_resp.SymbolID), np.uint32(m_resp.BidPrice * 100), np.uint32(m_resp.BidQuantity), np.uint32(m_resp.AskPrice * 100), np.uint32(m_resp.AskQuantity)])
            np.savetxt(prc_file, msg[None,:], fmt="%d", delimiter=', ')

        elif value[0] == 112:
            m_resp = Dtc.MarketDataUpdateTradeCompact() 
            m_resp.ParseFromString(value[1])
            msg = np.array([np.uint32(value[0]), np.uint32(m_resp.DateTime), np.uint32(m_resp.SymbolID), np.uint32(m_resp.Price * 100), np.uint32(m_resp.Volume), np.uint32(m_resp.AtBidOrAsk)])
            np.savetxt(prc_file, msg[None,:], fmt="%d", delimiter=', ')

        # add to stack buffer list

async def prc_feeder(client_stream, send_channel, loggen):
    # loggen.debug("prc_feeder started")
    logStdout.info("prc_feeder started")
    
    while True:
        m_type, m_body = await get_response(client_stream)
        #logStdout.info("%s", m_type)
        
        if m_type != 3:
            if m_type == 112 or m_type == 117:
                await send_channel.send((m_type, m_body))
            else:
                msg = read_response(m_type,m_body)
                loggen.info("%s, %s", m_type, msg)
        await trio.sleep(0.001)
    #prc_file.close()

def read_response(m_type, m_body):
    #m_type = DMAP[m_type]
    m_resp = DMAP[m_type][1]()
    m_resp.ParseFromString(m_body)
    #m_read = [m_type]
    m_read = tuple((f.name, getattr(m_resp,f.name), type(getattr(m_resp, f.name))) for f in m_resp.DESCRIPTOR.fields)
    return m_read


async def get_response(client_stream):
    header = await client_stream.receive_some(4)
    m_size = struct.unpack_from('<H', header[:2])[0] # the size of message
    m_type = struct.unpack_from('<H', header[2:4])[0] # the type of the message
    m_body = await client_stream.receive_some(m_size - 4)
    #m_type = DMAP[m_type]
    #m_resp = m_type[3]()
    #m_resp.ParseFromString(m_body)
    logStdout.info(m_type)
    #if m_type == 3: print("GOTTT THE HEARTBEATEDDDD")
    return m_type,m_body # mtype is now an int, mbody is bytes 

# -- NEW
async def keyboard():
    """Return an iterator of characters from stdin."""
    stashed_term = termios.tcgetattr(sys.stdin)
    try:
        tty.setcbreak(sys.stdin, termios.TCSANOW)
        while True:
            #yield await trio.run_sync_in_worker_thread(sys.stdin.read, 1,cancellable=True)
            yield await trio.to_thread.run_sync(sys.stdin.read, 1,cancellable=True)
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSANOW, stashed_term)

async def trigger(event,scope, sock, loggen, prc_file):
    loggen.info("  trigger waiting now...")
    logStdout.info("  trigger waiting now...")
    await event.wait()
    """ Gracefully logoff and close the connection """
    loggen.info(" QUIT!!! : Gracefully logoff and close the connection ")
    logStdout.info(" QUIT!!! : Gracefully logoff and close the connection ")
    logoff = Dtc.Logoff()
    logoff.Reason = "John-Client quit"
    await send_message(logoff, Dtc.LOGOFF, sock)
    prc_file.close()
    loggen.info('--------------------------------------------------------------')
    logStdout.info('--------------------------------------------------------------')
    scope.cancel()

async def parent(addr, symbols, logfile, encoding=Dtc.PROTOCOL_BUFFERS, heartbeat_interval=15):
    event = trio.Event()
    #logprc = create_logprc(logfile)
    prc_file = open(logfile+"_prc", 'a')
    loggen = create_loggen(logfile)
    loggen.info(f"connecting to {addr[0]}:{addr[1]}")
    logStdout.info(f"connecting to {addr[0]}:{addr[1]}")
    client_stream = await trio.open_tcp_stream(addr[0], addr[1])
    # client_stream = await trio.open_ssl_over_tcp_stream(addr[0], addr[1], ssl_context=ssl.SSLContext())

    async with client_stream:
        # encoding request
        loggen.info("spawing encoding request ...")
        # construct encoding request
        enc_req = Dtc.EncodingRequest()
        enc_req.Encoding = encoding
        enc_req.ProtocolType = "DTC"
        enc_req.ProtocolVersion = Dtc.CURRENT_VERSION
        await send_message(enc_req, Dtc.ENCODING_REQUEST,client_stream) # send encoding request
        mtype, mbody = await get_response(client_stream)
        msg = read_response(mtype,mbody)
        loggen.info("%s, %s", mtype, msg)

        # # logon request
        loggen.info("spawing logon request ...")
        logon_req = Dtc.LogonRequest()
        logon_req.ProtocolVersion = Dtc.CURRENT_VERSION
        # logon_req.Username = "wat"
        # logon_req.Password = "wat"
        logon_req.GeneralTextData = "John's Test"
        logon_req.HeartbeatIntervalInSeconds = heartbeat_interval
        logon_req.ClientName = "John_Tester"
        await send_message(logon_req, Dtc.LOGON_REQUEST, client_stream)
        mtype, mdata = await get_response(client_stream)
        msg = read_response(mtype,mbody)
        loggen.info("%s, %s", mtype, msg)

        # Start nursery
        async with trio.open_nursery() as nursery:
            # trigger
            loggen.info("spawning trigger...")
            nursery.start_soon(trigger, event, nursery.cancel_scope, client_stream, loggen, prc_file)
            
            # Open Channels to separate receiving and writing jobs from feed
            send_channel, receive_channel = trio.open_memory_channel(4096)
            # Start a producer and a consumer, passing one end of the channel to each of them:
            #nursery.start_soon(producer, send_channel)
            #nursery.start_soon(consumer, receive_channel)
            
            # mkt data request
            loggen.info("spawning mkt data request ...")
            for m in symbols:    
                data_req = create_mktdat_req(*m)
                await send_message(data_req, Dtc.MARKET_DATA_REQUEST, client_stream)
                mtype, mdata = await get_response(client_stream)
                msg = read_response(mtype,mbody)
                loggen.info("%s, %s", mtype, msg)

            loggen.info("spawning heartbeater ...")
            nursery.start_soon(heartbeater, client_stream, heartbeat_interval) # task to send heartbeat

            loggen.info("spawning prc_feeder ...")
            nursery.start_soon(prc_feeder, client_stream, send_channel, loggen) # task to grab price from feed
            nursery.start_soon(process_row, receive_channel, prc_file) # task to write to disk
            
            # -- NEW
            async for key in keyboard():
                if key == 'q':
                    # nursery.cancel_scope.cancel()
                    event.set()
                    
            loggen.info("waiting for tasks to finish...")
            logStdout.info("waiting for tasks to finish...")

        loggen.info("  Logoff Successful!!!")
        logStdout.info("  Logoff Successful!!!")

    loggen.info("  Socket Closed!!! --------------------------------------------------------")
    logStdout.info("  Socket Closed!!!------------------------------------------------------")


# Connection Params setup 
# addr0 = ("127.0.0.1", 11099)
# encoding = Dtc.PROTOCOL_BUFFERS
# encoding = Dtc.BINARY_ENCODING
# heartbeat_interval = 10

# trio.run(dtc_livelog1.parent, addr1, sym, "test_livelog1")
