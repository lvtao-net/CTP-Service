# -*- coding: utf-8 -*-

import json, datetime, time, logging, os, threading, re, aiohttp, hashlib
from sanic import Sanic, Blueprint, response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from collections import defaultdict
import ctpwrapper as CTP
import ctpwrapper.ApiStructure as CTPStruct

api = Blueprint('trade_ctp', url_prefix='/trade/ctp')

# 连接池存储所有客户端连接
connection_pool = {}

# MD5 生成函数
def generate_sid(trader_server, broker_id, investor_id, password):
    string_to_hash = f"{trader_server}{broker_id}{investor_id}{password}"
    return hashlib.md5(string_to_hash.encode('utf-8')).hexdigest()

# 全局变量
session = None
MAX_TIMEOUT = 10
DATA_DIR = "ctp_client_data/"
FILTER = lambda x: None if x > 1.797e+308 else x
logger = None
scheduler = None

# 从环境变量获取 base_url，默认为 http://127.0.0.1:7000
BASE_URL = os.environ.get('CTP_BASE_URL', 'http://127.0.0.1:7000')

@api.listener('before_server_start')
async def before_server_start(app, loop):
    global session, logger, scheduler
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar, connector=aiohttp.TCPConnector(ssl=False))

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    scheduler = AsyncIOScheduler()
    scheduler.start()

@api.listener('after_server_stop')
async def after_server_stop(app, loop):
    for sid in list(connection_pool.keys()):
        connection_pool[sid].logout()
        del connection_pool[sid]
    await session.close()
    scheduler.shutdown()

async def get_json(url, headers={}):
    async with session.get(url, headers=headers) as resp:
        return await resp.json()

class SpiHelper:
    def __init__(self):
        self._event = threading.Event()
        self._error = None

    def resetCompletion(self):
        self._event.clear()
        self._error = None

    def waitCompletion(self, operation_name=""):
        if not self._event.wait(MAX_TIMEOUT):
            raise TimeoutError("%s超时" % operation_name)
        if self._error:
            raise RuntimeError(self._error)

    def notifyCompletion(self, error=None):
        self._error = error
        self._event.set()

    def _cvtApiRetToError(self, ret):
        assert(-3 <= ret <= -1)
        return ("网络连接失败", "未处理请求超过许可数", "每秒发送请求数超过许可数")[-ret - 1]

    def checkApiReturn(self, ret):
        if ret != 0:
            raise RuntimeError(self._cvtApiRetToError(ret))

    def checkApiReturnInCallback(self, ret):
        if ret != 0:
            self.notifyCompletion(self._cvtApiRetToError(ret))

    def checkRspInfoInCallback(self, info):
        if not info or info.ErrorID == 0:
            return True
        self.notifyCompletion(info.ErrorMsg)
        return False

class QuoteImpl(SpiHelper, CTP.MdApiPy):
    def __init__(self, front):
        SpiHelper.__init__(self)
        CTP.MdApiPy.__init__(self)
        self._receiver = None
        flow_dir = DATA_DIR + "md_flow/"
        os.makedirs(flow_dir, exist_ok=True)
        self.Create(flow_dir)
        self.RegisterFront(front)
        self.Init()
        self.waitCompletion("登录行情会话")
    
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        print("OnRspError:")
        print("requestID:", nRequestID)
        print(pRspInfo)
        print(bIsLast)

    def __del__(self):
        self.Release()
        logger.info("已登出行情服务器...")

    def shutdown(self):
        self.Release()
        logger.info("已登出行情服务器...")

    def OnFrontConnected(self):
        logger.info("已连接行情服务器...")
        field = CTPStruct.ReqUserLoginField()
        self.checkApiReturnInCallback(self.ReqUserLogin(field, 0))
        self.status = 0
        
    def OnFrontDisconnected(self, nReason):
        logger.info("已断开行情服务器:{}...".format(nReason))
        print("Md OnFrontDisconnected {0}".format(nReason))
    
    def OnHeartBeatWarning(self, nTimeLapse):
        logger.info('Md OnHeartBeatWarning, time = {0}'.format(nTimeLapse))

    def OnRspUserLogin(self, _, info, req_id, is_last):
        assert(req_id == 0)
        assert(is_last)
        if not self.checkRspInfoInCallback(info):
            return
        logger.info("已登录行情会话...")
        self.status = 1
        self.notifyCompletion()

    def setReceiver(self, func):
        old_func = self._receiver
        self._receiver = func
        return old_func

    def subscribe(self, codes):
        self.resetCompletion()
        self.checkApiReturn(self.SubscribeMarketData(codes))
        self.waitCompletion("订阅行情")

    def OnRspSubMarketData(self, field, info, _, is_last):
        if not self.checkRspInfoInCallback(info):
            assert(is_last)
            return
        logger.info("已订阅<%s>的行情..." % field.InstrumentID)
        if is_last:
            self.notifyCompletion()

    def OnRtnDepthMarketData(self, field):
        if not self._receiver:
            return
        self._receiver({"trade_time": field.TradingDay[:4] + '-' + field.TradingDay[4:6] + '-' + field.TradingDay[6:] + " " + field.UpdateTime, 
                       "update_sec": int(field.UpdateMillisec), 
                       "code": field.InstrumentID, 
                       "price": FILTER(field.LastPrice),
                       "open": FILTER(field.OpenPrice), 
                       "close": FILTER(field.ClosePrice),
                       "highest": FILTER(field.HighestPrice), 
                       "lowest": FILTER(field.LowestPrice),
                       "upper_limit": FILTER(field.UpperLimitPrice),
                       "lower_limit": FILTER(field.LowerLimitPrice),
                       "settlement": FILTER(field.SettlementPrice), 
                       "volume": field.Volume,
                       "turnover": field.Turnover, 
                       "open_interest": int(field.OpenInterest),
                       "pre_close": FILTER(field.PreClosePrice),
                       "pre_settlement": FILTER(field.PreSettlementPrice),
                       "pre_open_interest": int(field.PreOpenInterest),
                       "ask1": (FILTER(field.AskPrice1), field.AskVolume1),
                       "bid1": (FILTER(field.BidPrice1), field.BidVolume1),
                       "ask2": (FILTER(field.AskPrice2), field.AskVolume2),
                       "bid2": (FILTER(field.BidPrice2), field.BidVolume2),
                       "ask3": (FILTER(field.AskPrice3), field.AskVolume3),
                       "bid3": (FILTER(field.BidPrice3), field.BidVolume3),
                       "ask4": (FILTER(field.AskPrice4), field.AskVolume4),
                       "bid4": (FILTER(field.BidPrice4), field.BidVolume4),
                       "ask5": (FILTER(field.AskPrice5), field.AskVolume5),
                       "bid5": (FILTER(field.BidPrice5), field.BidVolume5)})

    def unsubscribe(self, codes):
        self.resetCompletion()
        self.checkApiReturn(self.UnSubscribeMarketData(codes))
        self.waitCompletion("取消订阅行情")

    def OnRspUnSubMarketData(self, field, info, _, is_last):
        if not self.checkRspInfoInCallback(info):
            assert(is_last)
            return
        logger.info("已取消订阅<%s>的行情..." % field.InstrumentID)
        if is_last:
            self.notifyCompletion()

class TraderImpl(SpiHelper, CTP.TraderApiPy):
    def __init__(self, front, broker_id, app_id, auth_code, user_id, password):
        SpiHelper.__init__(self)
        CTP.TraderApiPy.__init__(self)
        self._last_query_time = 0
        self._broker_id = broker_id
        self._app_id = app_id
        self._auth_code = auth_code
        self._user_id = user_id
        self._password = password
        self._front_id = None
        self._session_id = None
        self._order_action = None
        self._order_ref = 0
        flow_dir = DATA_DIR + "td_flow/"
        os.makedirs(flow_dir, exist_ok=True)
        self.Create(flow_dir)
        self.RegisterFront(front)
        self.SubscribePrivateTopic(2)
        self.SubscribePublicTopic(2)
        self.Init()
        self.waitCompletion("登录交易会话")
        del self._app_id, self._auth_code, self._password
        self._getInstruments()
        self.instruments_option = defaultdict(list)
        self.instruments_future = defaultdict(list)
        self._buildInstrumentsDict()

    def _limitFrequency(self):
        delta = time.time() - self._last_query_time
        if delta < 1:
            time.sleep(1 - delta)
        self._last_query_time = time.time()

    def __del__(self):
        self.Release()
        logger.info("已登出交易服务器...")
    
    def shutdown(self):
        self.Release()
        logger.info("已登出交易服务器...")

    def OnFrontConnected(self):
        logger.info("已连接交易服务器...")
        field = CTPStruct.ReqAuthenticateField(BrokerID=self._broker_id,
                AppID=self._app_id, AuthCode=self._auth_code, UserID=self._user_id)
        self.checkApiReturnInCallback(self.ReqAuthenticate(field, 0))
    
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        print("OnRspError:")
        print("requestID:", nRequestID)
        print(pRspInfo)
        print(bIsLast)

    def OnHeartBeatWarning(self, nTimeLapse):
        logger.info("OnHeartBeatWarning time: ", nTimeLapse)

    def OnFrontDisconnected(self, nReason):
        logger.info("已断开交易服务器:{}...".format(nReason))
        print("OnFrontDisConnected:", nReason)

    def OnRspAuthenticate(self, _, info, req_id, is_last):
        assert(req_id == 0)
        assert(is_last)
        if not self.checkRspInfoInCallback(info):
            return
        logger.info("已通过交易终端认证...")
        field = CTPStruct.ReqUserLoginField(BrokerID=self._broker_id,
                UserID=self._user_id, Password=self._password)
        self.checkApiReturnInCallback(self.ReqUserLogin(field, 1))

    def OnRspUserLogin(self, field, info, req_id, is_last):
        assert(req_id == 1)
        assert(is_last)
        if not self.checkRspInfoInCallback(info):
            return
        self._front_id = field.FrontID
        self._session_id = field.SessionID
        logger.info("已登录交易会话...")
        field = CTPStruct.SettlementInfoConfirmField(BrokerID=self._broker_id,
                InvestorID=self._user_id)
        self.checkApiReturnInCallback(self.ReqSettlementInfoConfirm(field, 2))

    def OnRspSettlementInfoConfirm(self, _, info, req_id, is_last):
        assert(req_id == 2)
        assert(is_last)
        if not self.checkRspInfoInCallback(info):
            return
        logger.info("已确认结算单...")
        self.notifyCompletion()

    def _getInstruments(self):
        file_path = DATA_DIR + "instruments.dat"
        now_date = time.strftime("%Y-%m-%d", time.localtime())
        if os.path.exists(file_path):
            fd = open(file_path)
            cached_date = fd.readline()
            if cached_date[: -1] == now_date:
                self._instruments = json.load(fd)
                fd.close()
                logger.info("已加载全部共%d个合约..." % len(self._instruments))
                return
            fd.close()
        self._instruments = {}
        self.resetCompletion()
        self._limitFrequency()
        self.checkApiReturn(self.ReqQryInstrument(CTPStruct.QryInstrumentField(), 3))
        last_count = 0
        while True:
            try:
                self.waitCompletion("获取所有合约")
                break
            except TimeoutError as e:
                count = len(self._instruments)
                if count == last_count:
                    raise e
                logger.info("已获取%d个合约..." % count)
                last_count = count
        fd = open(file_path, "w")
        fd.write(now_date + "\n")
        json.dump(self._instruments, fd, ensure_ascii=False)
        fd.close()
        logger.info("已保存全部共%d个合约..." % len(self._instruments))
    
    def _buildInstrumentsDict(self):
        for symbol in self._instruments:
            instrument = self._instruments[symbol]
            instrument["symbol"] = symbol
            if re.search(r"[\d\-][CP][\d\-]", symbol):
                try:
                    self.instruments_option[re.findall(r"([A-Za-z]{2,}\d{2,})", symbol)[0]].append(instrument)
                except:
                    self.instruments_option[re.findall(r'(^[A-Za-z]\d+)', symbol)[0]].append(instrument)
            else:
                self.instruments_future[instrument['exchange']].append(instrument)

    def OnRspQryInstrument(self, field, info, req_id, is_last):
        assert(req_id == 3)
        if not self.checkRspInfoInCallback(info):
            assert(is_last)
            return
        if field:
            if field.OptionsType == '1':
                option_type = "call"
            elif field.OptionsType == '2':
                option_type = "put"
            else:
                option_type = None
            expire_date = None if field.ExpireDate == "" else \
                    time.strftime("%Y-%m-%d", time.strptime(field.ExpireDate, "%Y%m%d"))
            self._instruments[field.InstrumentID] = {"name": field.InstrumentName,
                    "exchange": field.ExchangeID, "multiple": field.VolumeMultiple,
                    "price_tick": field.PriceTick, "expire_date": expire_date,
                    "long_margin_ratio": FILTER(field.LongMarginRatio),
                    "short_margin_ratio": FILTER(field.ShortMarginRatio),
                    "option_type": option_type, "strike_price": FILTER(field.StrikePrice),
                    "is_trading": bool(field.IsTrading)}
        if is_last:
            logger.info("已获取全部共%d个合约..." % len(self._instruments))
            self.notifyCompletion()

    def getAccount(self):
        field = CTPStruct.QryTradingAccountField(BrokerID=self._broker_id,
                InvestorID=self._user_id, CurrencyID="CNY", BizType='1')
        self.resetCompletion()
        self._limitFrequency()
        self.checkApiReturn(self.ReqQryTradingAccount(field, 8))
        self.waitCompletion("获取资金账户")
        return self._account

    def OnRspQryTradingAccount(self, field, info, req_id, is_last):
        assert(req_id == 8)
        assert(is_last)
        if not self.checkRspInfoInCallback(info):
            return
        self._account = {"balance": field.Balance, "margin": field.CurrMargin,
                "available": field.Available}
        logger.info("已获取资金账户...")
        self.notifyCompletion()

    def getOrders(self):
        self._orders = {}
        field = CTPStruct.QryOrderField(BrokerID=self._broker_id,
                InvestorID=self._user_id)
        self.resetCompletion()
        self._limitFrequency()
        self.checkApiReturn(self.ReqQryOrder(field, 4))
        self.waitCompletion("获取所有报单")
        return self._orders

    def _gotOrder(self, order):
        if len(order.OrderSysID) == 0:
            return
        oid = "%s@%s" % (order.OrderSysID, order.InstrumentID)
        (direction, volume) = (int(order.Direction), order.VolumeTotalOriginal)
        assert(direction in (0, 1))
        if order.CombOffsetFlag == '1':
            direction = 1 - direction
            volume = -volume
        direction = "short" if direction else "long"
        is_active = order.OrderStatus not in ('0', '5')
        assert(oid not in self._orders)
        self._orders[oid] = {"code": order.InstrumentID, "direction": direction,
                "price": order.LimitPrice, "volume": volume,
                "volume_traded": order.VolumeTraded, "is_active": is_active}

    def OnRspQryOrder(self, field, info, req_id, is_last):
        assert(req_id == 4)
        if not self.checkRspInfoInCallback(info):
            assert(is_last)
            return
        if field:
            self._gotOrder(field)
        if is_last:
            logger.info("已获取所有报单...")
            self.notifyCompletion()

    def getPositions(self):
        self._positions = []
        field = CTPStruct.QryInvestorPositionField(BrokerID=self._broker_id,
                InvestorID=self._user_id)
        self.resetCompletion()
        self._limitFrequency()
        self.checkApiReturn(self.ReqQryInvestorPosition(field, 5))
        self.waitCompletion("获取所有持仓")
        return self._positions

    def _gotPosition(self, position):
        code = position.InstrumentID
        if position.PosiDirection == '2':
            direction = "long"
        elif position.PosiDirection == '3':
            direction = "short"
        else:
            return
        volume = position.Position
        if volume == 0:
            return
        self._positions.append({"code": code, "direction": direction,
                    "volume": volume, "margin": position.UseMargin,
                    "cost": position.OpenCost})

    def OnRspQryInvestorPosition(self, field, info, req_id, is_last):
        assert(req_id == 5)
        if not self.checkRspInfoInCallback(info):
            assert(is_last)
            return
        if field:
            self._gotPosition(field)
        if is_last:
            logger.info("已获取所有持仓...")
            self.notifyCompletion()

    def OnRtnOrder(self, order):
        if self._order_action:
            if self._order_action(order):
                self._order_action = None

    def _handleNewOrder(self, order):
        order_ref = None if len(order.OrderRef) == 0 else int(order.OrderRef)
        if (order.FrontID, order.SessionID, order_ref) != \
                (self._front_id, self._session_id, self._order_ref):
            return False
        logging.debug(order)
        if order.OrderStatus == 'a':
            return False
        if order.OrderSubmitStatus == '4':
            self.notifyCompletion(order.StatusMsg)
            return True
        if order.TimeCondition == '1':
            if order.OrderStatus in ('0', '5'):
                logger.info("已执行IOC单，成交量：%d" % order.VolumeTraded)
                self._traded_volume = order.VolumeTraded
                self.notifyCompletion()
                return True
        else:
            assert(order.TimeCondition == '3')
            if order.OrderSubmitStatus == '3':
                assert(order.OrderStatus in ('0', '1', '2', '3', '4', '5'))
                assert(len(order.OrderSysID) != 0)
                self._order_id = "%s@%s" % (order.OrderSysID, order.InstrumentID)
                logger.info("已提交限价单（单号：<%s>）" % self._order_id)
                self.notifyCompletion()
                return True
        return False

    def _order(self, code, direction, volume, price, min_volume):
        if code not in self._instruments:
            raise ValueError("合约<%s>不存在！" % code)
        exchange = self._instruments[code]["exchange"]
        if direction == "long":
            direction = 0
        elif direction == "short":
            direction = 1
        else:
            raise ValueError("错误的买卖方向<%s>" % direction)
        if volume != int(volume) or volume == 0:
            raise ValueError("交易数量<%s>必须是非零整数" % volume)
        if volume > 0:
            offset_flag = '0'
        else:
            offset_flag = '1'
            volume = -volume
            direction = 1 - direction
        direction = str(direction)
        if price == 0:
            if exchange == "CFFEX":
                price_type = 'G'
            else:
                price_type = '1'
            (time_cond, volume_cond) = ('1', '1')
        elif min_volume == 0:
            (price_type, time_cond, volume_cond) = ('2', '3', '1')
        else:
            min_volume = abs(min_volume)
            if min_volume > volume:
                raise ValueError("最小成交量<%s>不能超过交易数量<%s>" % (min_volume, volume))
            (price_type, time_cond, volume_cond) = ('2', '1', '2')
        self._order_ref += 1
        self._order_action = self._handleNewOrder
        field = CTPStruct.InputOrderField(BrokerID=self._broker_id,
                InvestorID=self._user_id, ExchangeID=exchange, InstrumentID=code,
                Direction=direction, CombOffsetFlag=offset_flag,
                TimeCondition=time_cond, VolumeCondition=volume_cond,
                OrderPriceType=price_type, LimitPrice=price,
                VolumeTotalOriginal=volume, MinVolume=min_volume,
                CombHedgeFlag='1',
                ContingentCondition='1',
                ForceCloseReason='0',
                OrderRef="%12d" % self._order_ref)
        self.resetCompletion()
        self.checkApiReturn(self.ReqOrderInsert(field, 6))
        self.waitCompletion("录入报单")

    def OnRspOrderInsert(self, field, info, req_id, is_last):
        assert(req_id == 6)
        assert(is_last)
        self.OnErrRtnOrderInsert(field, info)

    def OnErrRtnOrderInsert(self, _, info):
        success = self.checkRspInfoInCallback(info)
        assert(not success)

    def orderMarket(self, code, direction, volume):
        self._order(code, direction, volume, 0, 0)
        return self._traded_volume

    def orderFAK(self, code, direction, volume, price, min_volume):
        assert(price > 0)
        self._order(code, direction, volume, price, 1 if min_volume == 0 else min_volume)
        return self._traded_volume

    def orderFOK(self, code, direction, volume, price):
        return self.orderFAK(code, direction, volume, price, volume)

    def orderLimit(self, code, direction, volume, price):
        assert(price > 0)
        self._order(code, direction, volume, price, 0)
        return self._order_id

    def _handleDeleteOrder(self, order):
        oid = "%s@%s" % (order.OrderSysID, order.InstrumentID)
        if oid != self._order_id:
            return False
        logging.debug(order)
        if order.OrderSubmitStatus == '5':
            self.notifyCompletion(order.StatusMsg)
            return True
        if order.OrderStatus in ('0', '5'):
            logger.info("已撤销限价单，单号：<%s>" % self._order_id)
            self.notifyCompletion()
            return True
        return False

    def deleteOrder(self, order_id):
        items = order_id.split("@")
        if len(items) != 2:
            raise ValueError("订单号<%s>格式错误" % order_id)
        (sys_id, code) = items
        if code not in self._instruments:
            raise ValueError("订单号<%s>中的合约号<%s>不存在" % (order_id, code))
        field = CTPStruct.InputOrderActionField(BrokerID=self._broker_id,
                InvestorID=self._user_id, UserID=self._user_id,
                ActionFlag='0',
                ExchangeID=self._instruments[code]["exchange"],
                InstrumentID=code, OrderSysID=sys_id)
        self.resetCompletion()
        self._order_id = order_id
        self._order_action = self._handleDeleteOrder
        self.checkApiReturn(self.ReqOrderAction(field, 7))
        self.waitCompletion("撤销报单")

    def OnRspOrderAction(self, field, info, req_id, is_last):
        assert(req_id == 7)
        assert(is_last)
        self.OnErrRtnOrderAction(field, info)

    def OnErrRtnOrderAction(self, _, info):
        success = self.checkRspInfoInCallback(info)
        assert(not success)

class Client:
    def __init__(self, md_front, td_front, broker_id, app_id, auth_code, user_id, password):
        self._md = None
        self._td = None
        self.md_front = md_front
        self.td_front = td_front
        self.broker_id = broker_id
        self.app_id = app_id
        self.auth_code = auth_code
        self.user_id = user_id
        self.password = password
        self.sid = generate_sid(td_front, broker_id, user_id, password)
    
    def login(self):
        self._td = TraderImpl(self.td_front, self.broker_id, self.app_id, self.auth_code, self.user_id, self.password)
        self._md = QuoteImpl(self.md_front)
    
    def logout(self):
        if self._md:
            self._md.shutdown()
        if self._td:
            self._td.shutdown()
    
    def setReceiver(self):
        try:
            from hq_func import parse_hq
        except:
            parse_hq = lambda x: print(x)
        return self._md.setReceiver(parse_hq)

    def subscribe(self, codes):
        for code in codes:
            if code not in self._td._instruments:
                raise ValueError("合约<%s>不存在" % code)
        self._md.subscribe(codes)

    def get_instruments_option(self, future=None):
        if future is None:
            return self._td.instruments_option
        return self._td.instruments_option.get(future, None)

    def get_instruments_future(self, exchange=None):
        if exchange is None:
            return self._td.instruments_future
        return self._td.instruments_future[exchange]

    def unsubscribe(self, codes):
        self._md.unsubscribe(codes)

    def getInstrument(self, code):
        if code not in self._td._instruments:
            raise ValueError("合约<%s>不存在" % code)
        return self._td._instruments[code].copy()

    def getAccount(self):
        return self._td.getAccount()

    def getOrders(self):
        return self._td.getOrders()

    def getPositions(self):
        return self._td.getPositions()

    def orderMarket(self, code, direction, volume):
        return self._td.orderMarket(code, direction, volume)

    def orderFAK(self, code, direction, volume, price, min_volume):
        return self._td.orderFAK(code, direction, volume, price, min_volume)

    def orderFOK(self, code, direction, volume, price):
        return self._td.orderFOK(code, direction, volume, price)

    def orderLimit(self, code, direction, volume, price):
        return self._td.orderLimit(code, direction, volume, price)

    def deleteOrder(self, order_id):
        self._td.deleteOrder(order_id)

# SID 验证中间件
async def validate_sid(request):
    sid = request.args.get('sid')
    if not sid or sid not in connection_pool:
        return response.json({"error": "无效的会话ID"}, status=400)
    return None

@api.route('/create_connection', methods=['GET'])
async def create_connection(request):
    try:
        investor_id = request.args.get("investor_id")
        broker_id = request.args.get("broker_id")
        password = request.args.get("password")
        md_server = request.args.get("md_server")
        trader_server = request.args.get("trader_server")
        app_id = request.args.get("app_id")
        auth_code = request.args.get("auth_code")

        if not all([investor_id, broker_id, password, md_server, trader_server, app_id, auth_code]):
            return response.json({"error": "缺少必要参数"}, status=400)

        sid = generate_sid(trader_server, broker_id, investor_id, password)
        
        if sid not in connection_pool:
            client = Client(md_server, trader_server, broker_id, app_id, auth_code, investor_id, password)
            client.login()
            connection_pool[sid] = client
            
            now = datetime.datetime.now()
            scheduler.add_job(lambda: login_request(sid), 'cron', id=f'job_login_{sid}', 
                            day_of_week='mon,tue,wed,thu,fri', hour='8,20', minute=40, second=0)
            scheduler.add_job(lambda: logout_request(sid), 'cron', id=f'job_logout_{sid}', 
                            day_of_week='mon,tue,wed,thu,fri,sat', hour='15,2', minute=40, second=0)
            
            if (now.strftime("%H:%M") > '08:40' and now.strftime("%H:%M") < '14:55') or \
               (now.strftime("%H:%M") > '20:40' or now.strftime("%H:%M") < '02:25') and now.weekday() < 6:
                scheduler.add_job(lambda: login_request(sid), trigger='date', 
                                next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=10), 
                                id=f"pad_task_{sid}")

        return response.json({"sid": sid, "time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

async def login_request(sid):
    return await get_json(f"{BASE_URL}/trade/ctp/login?sid={sid}")

async def logout_request(sid):
    return await get_json(f"{BASE_URL}/trade/ctp/logout?sid={sid}")

@api.route('/login', methods=['GET'])
async def login(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    try:
        connection_pool[sid].login()
        return response.json({"time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/logout', methods=['GET'])
async def logout(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    try:
        connection_pool[sid].logout()
        return response.json({"time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_account', methods=['GET'])
async def get_account(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    try:
        data = connection_pool[sid].getAccount()
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_postion', methods=['GET'])
async def get_postion(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    try:
        data = connection_pool[sid].getPositions()
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/order_limit', methods=['GET'])
async def order_limit(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    code = request.args.get("code")
    direction = request.args.get("direction", "long")
    volume = int(request.args.get("volume", 1))
    price = float(request.args.get("price", "0"))
    try:
        data = connection_pool[sid].orderLimit(code, direction, volume, price)
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/order_market', methods=['GET'])
async def order_market(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    code = request.args.get("code")
    direction = request.args.get("direction", "long")
    volume = int(request.args.get("volume", 1))
    try:
        data = connection_pool[sid].orderMarket(code, direction, volume)
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/order_delete', methods=['GET'])
async def order_delete(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    order_id = request.args.get("order_id")
    try:
        data = connection_pool[sid].deleteOrder(order_id)
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_orders', methods=['GET'])
async def get_orders(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    try:
        data = connection_pool[sid].getOrders()
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_instruments_future', methods=['GET'])
async def get_instruments_future(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    exchange = request.args.get("exchange", "")
    try:
        if exchange == "":
            data = connection_pool[sid].get_instruments_future()
        else:
            data = connection_pool[sid].get_instruments_future(exchange)
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_instruments_option', methods=['GET'])
async def get_instruments_option(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    future = request.args.get("future", "")
    try:
        if future == "":
            data = connection_pool[sid].get_instruments_option()
        else:
            data = connection_pool[sid].get_instruments_option(future)
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/get_instruments_detail', methods=['GET'])
async def get_instruments_detail(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    code = request.args.get("code", "")
    try:
        if code != "":
            data = connection_pool[sid].getInstrument(code)
        else:
            data = {}
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/subscribe', methods=['GET'])
async def subscribe(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    codes = request.args.get("codes")
    try:
        if codes != "":
            data = connection_pool[sid].subscribe(codes.split(','))
            connection_pool[sid].setReceiver()
        else:
            data = {}
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/unsubscribe', methods=['GET'])
async def unsubscribe(request):
    if error := await validate_sid(request):
        return error
    sid = request.args.get('sid')
    codes = request.args.get("codes")
    try:
        if codes != "":
            data = connection_pool[sid].unsubscribe(codes.split(','))
        else:
            data = {}
        return response.json(data, ensure_ascii=False)
    except Exception as e:
        return response.json({"error": str(e)}, ensure_ascii=False)

@api.route('/market/event', methods=['GET'])
async def market_event(request):
    event_date = request.args.get("event_date", "")
    if event_date == '':
        event_date = datetime.date.today()
    else:
        event_date = datetime.date.fromisoformat(event_date)
    url = 'https://cdn-rili.jin10.com/web_data/{}/daily/{}/{}/economics.json'.format(event_date.year, event_date.month, event_date.day)
    headers = {'x-app-id': 'bVBF4FyRTn5NJF5n', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', 'x-version': '1.0.0', 'accept': 'application/json, text/plain, */*', 'referer': 'https://rili.jin10.com/', 'authority': 'cdn-rili.jin10.com'}
    data = await get_json(url, headers=headers)
    return response.json(data, ensure_ascii=False)

@api.route('/market/news', methods=['GET'])
async def market_news(request):
    max_date = request.args.get("max_date", "")
    if max_date == "":
        max_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    url = 'https://flash-api.jin10.com/get_flash_list?channel=-8200&max_time={}&vip=1'.format(max_date)
    headers = {'x-app-id': 'bVBF4FyRTn5NJF5n', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', 'x-version': '1.0.0', 'accept': 'application/json, text/plain, */*', 'referer': 'https://www.jin10.com/', 'authority': 'flash-api.jin10.com'}
    data = await get_json(url, headers=headers)
    return response.json(data, ensure_ascii=False)

@api.route('/market/realtime_hq', methods=['GET'])
async def market_realtime_hq(request):
    code = request.args.get('code', 'CNH')
    url = 'https://centerapi.fx168api.com/app/api/QuoteOrder/GetQuoteInfoList?quoteCode={}&showArea=1'.format(code)
    headers = {'authority': 'centerapi.fx168api.com', 'accept': 'application/json, text/plain, */*', 'referer': 'https://www.fx168news.com/', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
    data = await get_json(url, headers=headers)
    return response.json(data, ensure_ascii=False)

@api.route('/market/realtime_snap', methods=['GET'])
async def market_realtime_snap(request):
    dtype = request.args.get('dtype', '金属钢材')
    category = {"金属钢材": "003002", "能源化工": "003003", "农产品": "003004", "中金所": "011001005", "上期所": "011001001", "上期能源": "011001002", "大商所": "011001003", "郑商所": "011001004", "纽约NYMEX": "011002001", "纽约COMEX": "011002002", "芝加哥CBOT": "011002003", "芝加哥CME": "011002004", "芝加哥CBOE": "011002005", "伦敦LME": "011002006", "洲际ICE": "011002007", "东京TOCM": "011002008", "香港HKEX": "011002009", "股指": "007001", "外汇": "002001", "加密货币": "008", "债券": "009"}.get(dtype)
    url = 'https://centerapi.fx168api.com/app/api/QuoteOrder/GetQuoteInfoByCategoryCode?categoryCode={}&pageNo=1&pageSize=200&showArea=1'.format(category)
    headers = {'authority': 'centerapi.fx168api.com', 'accept': 'application/json, text/plain, */*', 'referer': 'https://www.fx168news.com/', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
    data = await get_json(url, headers=headers)
    return response.json(data, ensure_ascii=False)

@api.route('/market/realtime_dayline', methods=['GET'])
async def market_realtime_dayline(request):
    code = request.args.get('code', 'MTWTI0')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    if start_date == '':
        start_date = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    if end_date == '':
        end_date = datetime.date.today().strftime("%Y-%m-%d")
    url = 'https://centerapi.fx168api.com/app/api/TradingInterface/history?symbol={}&resolution=D&from={}&to={}&firstDataRequest=false'.format(code, int(1000 * datetime.datetime.fromisoformat(start_date).timestamp()), int(1000 * datetime.datetime.fromisoformat(end_date).timestamp()))
    headers = {'authority': 'centerapi.fx168api.com', 'accept': 'application/json, text/plain, */*', 'referer': 'https://www.fx168news.com/', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
    data = await get_json(url, headers=headers)
    return response.json(data, ensure_ascii=False)

app = Sanic(name=__name__)
app.config.RESPONSE_TIMEOUT = 6000000
app.config.REQUEST_TIMEOUT = 6000000
app.config.KEEP_ALIVE_TIMEOUT = 600000
app.blueprint(api)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000, workers=1, debug=True, auto_reload=True)
