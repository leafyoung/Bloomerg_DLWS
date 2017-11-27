import sys
import time
import socket
if sys.version_info[0] < 3:
    import urllib2 as urlreq
    import httplib as httpclient
else:
    import urllib.request as urlreq
    import http.client as httpclient
    
# Works with suds with python 2.7, suds-jurko with python 3.6
from suds.client import Client
from suds.transport.http import HttpTransport, Reply, TransportError

class HTTPSClientAuthHandler(urlreq.HTTPSHandler):
    def __init__(self, key, cert):
        urlreq.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httpclient.HTTPSConnection(host,
                                       key_file=self.key,
                                       cert_file=self.cert)

class HTTPSClientCertTransport(HttpTransport):
    def __init__(self, key, cert, *args, **kwargs):
        HttpTransport.__init__(self, *args, **kwargs)
        self.key = key
        self.cert = cert

    def u2open(self, u2request):
        """
        Open a connection.
        @param u2request: A urllib2/url.request request.
        @type u2request: urllib2.Requet//url.request.
        @return: The opened file-like urllib2 object.
        @rtype: fp
        """
        tm = self.options.timeout
        url = urlreq.build_opener(HTTPSClientAuthHandler(self.key, self.cert))
        if self.u2ver() < 2.6:
            socket.setdefaulttimeout(tm)
            return url.open(u2request)
        else:
            return url.open(u2request, timeout=tm)

def prepare_key():
    password = "<PASSWORD>"
    p12 = load_pkcs12(open("DLWSCert.p12", 'rb').read(), password)
    certFile = open("cert.pem", "wb")
    keyFile = open("key.pem", "wb")

    certFile.write(dump_certificate(FILETYPE_PEM, p12.get_certificate()))
    keyFile.write(dump_privatekey(FILETYPE_PEM, p12.get_privatekey()))

    certFile.close()
    keyFile.close()

def main():
    # These lines enable debug logging; remove them once everything works.
    import logging
    # logging.basicConfig(level=logging.INFO)
    # logging.getLogger('suds.client').setLevel(logging.DEBUG)
    # logging.getLogger('suds.transport').setLevel(logging.DEBUG)
    
    wsdl_uri = 'https://service.bloomberg.com/assets/dl/dlws.wsdl'
    c = Client(wsdl_uri, transport = HTTPSClientCertTransport('key.pem', 'cert.pem'))
    # print(c)

    submitGetDataReq = c.factory.create('SubmitGetDataRequest')

    # define the header section of the request
    reqHeaders = c.factory.create('GetDataHeaders')

    reqHeaders.dateformat = None
    reqHeaders.diffflag = None
    reqHeaders.secid = None
    reqHeaders.specialchar = None
    reqHeaders.version = None
    reqHeaders.yellowkey = None
    reqHeaders.bvaltier = None
    reqHeaders.bvalsnapshot = None
    reqHeaders.portsecdes = None
    reqHeaders.secmaster = True
    
    reqHeaders.closingvalues = True
    reqHeaders.derived = None
    progFlag = c.factory.create('ProgramFlag')
    reqHeaders.programflag = None # progFlag.oneshot
    regSolvency = c.factory.create('RegSolvency')
    reqHeaders.regsolvency = None # regSolvency.no

    submitGetDataReq.headers = reqHeaders

    # define the list of fields that you wish to have for all the tickers
    reqFields = c.factory.create('Fields')
    reqFields.field.append('PX_LAST')
    reqFields.field.append('PX_BID')
    reqFields.field.append('PX_ASK')

    submitGetDataReq.fields = reqFields

    bvalFieldSets = c.factory.create("BvalFieldSets")

    # define the tickers you wish to get data for
    reqInstruments = c.factory.create('Instruments')

    ticker2 = c.factory.create('Instrument')
    ticker2.id = 'SCOZ7'
    marketSector = c.factory.create('MarketSector')
    ticker2.yellowkey = marketSector.Comdty
    instrumentType = c.factory.create('InstrumentType')
    ticker2.type = None # instrumentType.TICKER
    reqInstruments.instrument.append(ticker2)

    ticker = c.factory.create('Instrument')
    ticker.id = 'IOEK8 ELEC'
    marketSector = c.factory.create('MarketSector')
    ticker.yellowkey = marketSector.Comdty
    ticker.type = None 
    reqInstruments.instrument.append(ticker)
    
    ticker = c.factory.create('Instrument')
    ticker.id = 'IOEK8 PIT'
    marketSector = c.factory.create('MarketSector')
    ticker.yellowkey = marketSector.Comdty
    ticker.type = None 
    reqInstruments.instrument.append(ticker)

    submitGetDataReq.instruments = reqInstruments

    print(submitGetDataReq)
    # send the request to request to Bloomberg Web Services interface.
    req = c.service.submitGetDataRequest(reqHeaders, bvalFieldSets, reqFields, reqInstruments)

    if req.statusCode.code != 0 or req.statusCode.description != "Success":
        # raise Exception.
        return()

    print("Request Done!")
    print(req.statusCode.code)
    print(req.statusCode.description)
    print(req.responseId)
    print(req.requestId)

    resp = c.service.retrieveGetDataResponse(req.responseId)
    while resp.statusCode.code != 0:
        time.sleep(10)
        resp = c.service.retrieveGetDataResponse(req.responseId)
        print(resp)

if __name__ == "__main__":
    main()