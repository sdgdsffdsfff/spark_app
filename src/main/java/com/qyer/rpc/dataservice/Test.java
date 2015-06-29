package com.qyer.rpc.dataservice;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by wangzhen on 15/6/29.
 */
public class Test {

    public CityInfo ip2CityId(String ip) throws TException {
        long t1 = System.currentTimeMillis();
        TTransport tTransport = null;
        try {
            tTransport = new TSocket("master", 9527);
            tTransport.open();
            TProtocol protocol = new TBinaryProtocol(tTransport);
            CommonServiceServer.Client client = new CommonServiceServer.Client(protocol);
            CityInfo cityInfo = client.ip2City(ip);
            if (cityInfo != null && cityInfo.getCityCode() == 0 && cityInfo.getCountryCode() == 0) {
                System.out.print("Unknown ip - " + ip);
            }
            return cityInfo;
        } finally {
            if (tTransport != null) {
                tTransport.close();
            }
        }
    }

    public static void main(String[] args) {
        Test test=new Test();
        try{
            System.out.print(test.ip2CityId("124.65.163.69").getCity());
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}
