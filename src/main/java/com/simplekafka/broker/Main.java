package com.simplekafka.broker;

public class Main {
    public static void main(String[] args) throws Exception {
        int id = Integer.parseInt(args[0]);
        BrokerInfo info = new BrokerInfo(id, "localhost", 9092 + id);
        Broker broker = new Broker(info, "localhost:2181");
        broker.start();
        System.out.println("Broker " + id + " arrancado. Es controller? " + broker.isController());
        Thread.sleep(10000);
    }
}
