package uiuc.mp2;

import com.google.gson.Gson;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by haowenjiang on 10/5/17.
 */
public class ProgressStart {
    public Node[] memberList;
    private boolean isIntroducer;
    public Node locNode;
    public ArrayList<Node> successors;
    public ArrayList<Node> predecessors;
    boolean locexist;
    String itdIP;
    Gson gson_send = new Gson();
    Gson gson_receive = new Gson();
    DatagramSocket ds_send;
    DatagramPacket dp_send;
    HashMap<Integer, Long> heartbeatcount;

    byte[] buf_send = new byte[10240];
    byte[] sendByte;
    byte[] buf_receive = new byte[10240];
    byte[] receiveByte;

    public ProgressStart(boolean isIntroducer){
        this.isIntroducer = isIntroducer;
    }


    // Design a Node on the virtual ring
    private class Node {
        public String ip;
        public String timestamp;
        public int id;
        public Node() throws UnknownHostException {

            String locIP = InetAddress.getLocalHost().getHostAddress();
            SimpleDateFormat df  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String curTim = df.format(new Date());
            this.ip = locIP;
            this.timestamp = curTim;
            String str = this.ip + this.timestamp;
            int ID = 5;
            for (int i =0; i < str.length(); i++){
                ID = ID * 31 + str.charAt(i);
            }

            this.id = Math.abs(ID % 256);
            System.out.println(id);
            while (memberList[id] != null){
                id = (id + 1) % 256;
            }
        }
    }


    // Monitor the command from users
    public void start() throws IOException {
        String locIP = InetAddress.getLocalHost().getHostAddress();
//        SimpleDateFormat df  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String curTim = df.format(new Date());
//
//        Node node = new Node(locIP, curTim);

        if (isIntroducer) {
            System.out.println("This is the introduce server, waiting for joining in");
            System.out.println("The Introducer ip address is: " + locIP);
        } else {
            Scanner sc = new Scanner(System.in);
            System.out.println("Please enter your introducer's ip: ");
            itdIP = sc.next();
        }

        while (true){
            System.out.println("Please Enter Your Command Here");
            InputStream cmd = System.in;
            BufferedReader cmd_br = new BufferedReader(new InputStreamReader(cmd));
            String command = cmd_br.readLine();

            if (command.equals("list membership list")) {
                if (memberList != null) {
                    if (locexist){
                        System.out.println("Membership List Shows here: ");
                        for (Node nd : memberList) {
                            if (nd != null) {
                                System.out.println(nd.ip + nd.timestamp);
                            }
                        }
                    } else {
                        System.out.println("Please Join First!!!");
                    }
                }
            }

            if (command.equals("list id")){
                if (memberList != null) {
                    if (locexist) {
                        System.out.println("Local IP: " + locNode.ip + "Attend Time: " + locNode.timestamp);
                    }
                }
            }

            if (command.equals("join the group")){
                if (locexist) {
                    System.out.println("This machine has already in the system!");
                } else {
                    initDGM();
                    if(!isIntroducer) {
                        sendJoinInf(itdIP);
                    } else {
                        System.out.println("This is introducer");
                    }
                }
            }

            if (command.equals("leave the group")) {
                if (!locexist){
                    System.out.println("You Are Not In The System");
                } else {
                    memberList = null;
                    heartbeatcount.clear();
                    locexist = false;
                    System.out.println("Current machine left from the system");
                    for (Node successor : successors) {
                        sendLeaveInf(successor.ip);
                    }
                }
            }
        }
    }

    // Initial related argument and start other threads
    private void initDGM() throws UnknownHostException {
        memberList = new Node[256];
        locNode = new Node();
        successors = new ArrayList<>();
        predecessors = new ArrayList<>();
        locexist = true;
        new Thread(new ServerThread()).start();
        new Thread(new ClientThread()).start();
        new Thread(new FailureDectectionThread()).start();
        memberList[locNode.id] = locNode;
    }

    // Monitor the information from other machines
    private class ServerThread implements Runnable{

        @Override
        public void run() {
            try {
                DatagramSocket ds_receive = new DatagramSocket(3000);
                DatagramPacket dp_receive = new DatagramPacket(buf_receive, 10240);
                while (locexist) {
                    ds_receive.receive(dp_receive);
                    String receive_str = new String(dp_receive.getData(), 0, dp_receive.getLength());
                    InfType receive_inftype = gson_receive.fromJson(receive_str, InfType.class);
                    Node node;
                    String receiveIP = dp_receive.getAddress().getHostAddress();


                    if (receive_inftype.type.equals("join")){
                        System.out.println("receive join information");
                        if (isIntroducer) {
                            node = receive_inftype.list[0];
                            memberList[node.id] = node;
                            System.out.println("memberlist ready to send");

                            sendMemberlistInf(receiveIP);



                            for (Node successor : successors) {
                                sendAddInf(successor.ip, node);
                            }

                            initialEnvironment();
                            initialHeartBeatCount();

                            writeToLog("A new machine join into the system", node);
                        }
                    }

                    if (receive_inftype.type.equals("leave")){
                        node = receive_inftype.list[0];
                        if (memberList[node.id] != null) {
                            System.out.println(node.ip + "leaving");
                            memberList[node.id] = null;
                            for (Node successor : successors){
                                sendDeleteInf(successor.ip, node);
                            }

                            initialEnvironment();
                            initialHeartBeatCount();

                            writeToLog("This machine left from the system", node);
                        } else {
                            System.out.println(node.ip + " has already left");
                        }


                    }

                    if (receive_inftype.type.equals("heartbeat")){
                        node = receive_inftype.list[0];
                        if (heartbeatcount.containsKey(node.id)) {
                            heartbeatcount.put(node.id, System.currentTimeMillis());
                        }
                    }

                    if (receive_inftype.type.equals("add")){
                        node = receive_inftype.list[0];
                        if (memberList[node.id] == null) {
                            memberList[node.id] = node;
                            System.out.println("New machine join into the system");

                            for (Node successor : successors){
                                sendAddInf(successor.ip, node);
                            }

                            initialEnvironment();
                            initialHeartBeatCount();

                            writeToLog("A new machine join into the system", node);
                        }
                    }

                    if (receive_inftype.type.equals("delete")){
                        node = receive_inftype.list[0];

                        if (memberList[node.id] != null) {
                            memberList[node.id] = null;
                            for (Node successor : successors){
                                sendDeleteInf(successor.ip, node);
                            }

                            initialEnvironment();
                            initialHeartBeatCount();

                            writeToLog("This machine left from the system", node);
                        }


                    }

                    if (receive_inftype.type.equals("fail")){
                        node = receive_inftype.list[0];
                        if (memberList[node.id] != null) {
                            System.out.println("fail ip is " + node.ip);

                            heartbeatcount.remove(node.id);
                            memberList[node.id] = null;
                            for (Node successor : successors) {
                                sendFailInf(successor.ip, node);
                            }

                            initialEnvironment();
                            initialHeartBeatCount();

                            writeToLog("This machine died. Please check and reboot it", node);
                        }



                    }

                    if(receive_inftype.type.equals("memberlist")){
                        memberList = receive_inftype.list;
                        System.out.println("Got the current membership list");

                        initialEnvironment();
                        initialHeartBeatCount();
                    }
                    Thread.sleep(10);
                }

                ds_receive.close();
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Send heartbeat to successors
    private class ClientThread implements Runnable{

        @Override
        public void run() {
            while(locexist) {
                try {
                    for (Node nd : successors) {
                        sendHeartBeatInf(nd.ip);
                    }
                    Thread.sleep(100);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//            ds_send.close();
        }
    }

    // Detect if the predecessors are failed or not
    private  class FailureDectectionThread implements  Runnable{

        @Override
        public void run() {
            while (locexist){
                if (heartbeatcount != null) {
                    for (Integer ID : heartbeatcount.keySet()){
                        long currenttime = System.currentTimeMillis();
                        if (currenttime - heartbeatcount.get(ID) > 1000){
                            System.out.println("This machine died:" + memberList[ID].ip);
                            if (memberList[ID] != null) {
                                Node node = memberList[ID];
                                memberList[ID] = null;
                                heartbeatcount.remove(ID);
                                System.out.println("remove from the list");

                                for (Node successor : successors) {
                                    try {
                                        sendFailInf(successor.ip, node);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }

                                initialEnvironment();
                                initialHeartBeatCount();

                                try {
                                    writeToLog("This machine died", node);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Send Join information to Introducer
    private void sendJoinInf(String ip) throws IOException {
        if (ip == null) {
            return;
        }

        InetAddress joingoalIP = InetAddress.getByName(ip);
        Node[] joinlist = new Node[1];
        joinlist[0] = locNode;
        InfType join_type = new InfType("join", joinlist);
        String join_str = gson_send.toJson(join_type);
        sendByte = join_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, joingoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();
    }

    // Initial successors and predecessors
    private void initialEnvironment(){
        successors.clear();
        predecessors.clear();
        int suc_index = (locNode.id + 1) % 256;
        while (successors.size() < 5 && suc_index != locNode.id){
            if(memberList[suc_index] != null){
                successors.add(memberList[suc_index]);
            }
            suc_index = (suc_index + 1) % 256;
        }

        int pre_index = (256 + locNode.id - 1) % 256;
        while (predecessors.size() < 5 && pre_index != locNode.id){
            if(memberList[pre_index] != null){
                predecessors.add(memberList[pre_index]);
            }
            pre_index = (256 + pre_index - 1) % 256;
        }
    }

    // initial heartbeat Count
    private void initialHeartBeatCount(){
        heartbeatcount = new HashMap<Integer, Long>();

        for (Node predecessor: predecessors){
            if(!heartbeatcount.containsKey(predecessor.ip)){
                heartbeatcount.put(predecessor.id, System.currentTimeMillis());
            } else {
                System.out.println("This heartbeat has already in the hashmap");
            }
        }
    }

    //send leave information to successors
    private void sendLeaveInf(String ip) throws IOException {

        InetAddress leavegoalIP = InetAddress.getByName(ip);
        Node[] leavelist = new Node[1];
        leavelist[0] = locNode;
        InfType leave_type = new InfType("leave", leavelist);
        String leave_str = gson_send.toJson(leave_type);
        sendByte = leave_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, leavegoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();
    }

    // send heartbeat information to predecessors
    private void sendHeartBeatInf(String ip) throws IOException {
        if (ip == null) {
            return;
        }
        InetAddress heartbeatgoalIP = InetAddress.getByName(ip);
        Node[] heartbeatlist = new Node[1];
        heartbeatlist[0] = locNode;
        InfType heartbeat_type = new InfType("heartbeat", heartbeatlist);
        String heartbeat_str = gson_send.toJson(heartbeat_type);
        sendByte = heartbeat_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, heartbeatgoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();
    }

    // send failure information to predecessors
    private void sendFailInf(String ip, Node node) throws IOException {
        if (ip == null && node == null) {
            return;
        }

        InetAddress failgoalIP = InetAddress.getByName(ip);
        Node[] faillist = new Node[1];
        faillist[0] = node;
        InfType fail_type = new InfType("fail", faillist);
        String fail_str = gson_send.toJson(fail_type);
        sendByte= fail_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, failgoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();
    }

    //send Membership list to the follower
    private void sendMemberlistInf (String ip) throws IOException {
        InetAddress memberlistgoalIP = InetAddress.getByName(ip);
        InfType memberlist_type = new InfType("memberlist", memberList);
        String memberlist_str = gson_send.toJson(memberlist_type);
        sendByte = memberlist_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, memberlistgoalIP, 3000);
        ds_send.send(dp_send);
        System.out.println("send list successful");
        ds_send.close();
    }

    //send add information to the successors
    private void sendAddInf(String ip, Node node) throws IOException {
        if (ip == null && node == null) {
            return;
        }

        InetAddress addgoalIP = InetAddress.getByName(ip);
        Node[] addlist = new Node[1];
        addlist[0] = node;
        InfType add_type = new InfType("add", addlist);
        String add_str = gson_send.toJson(add_type);
        sendByte= add_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, addgoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();
    }

    // send delete information to the successors
    private void sendDeleteInf(String ip, Node node) throws IOException {
        if (ip == null && node == null) {
            return;
        }

        InetAddress deletegoalIP = InetAddress.getByName(ip);
        Node[] deletelist = new Node[1];
        deletelist[0] = node;
        InfType delete_type = new InfType("delete", deletelist);
        String delete_str = gson_send.toJson(delete_type);
        sendByte = delete_str.getBytes();
        ds_send = new DatagramSocket();
        dp_send = new DatagramPacket(sendByte, sendByte.length, deletegoalIP, 3000);
        ds_send.send(dp_send);
        ds_send.close();

    }

    // define the infType which has to arguments: String type and Node
    private class InfType{
        public String type;
        public Node[] list;

        public InfType(String type, Node[] list){
            this.type = type;
            this.list = list;
        }
    }

    // write the modification to the log file
    private void writeToLog(String event, Node node) throws IOException {
        SimpleDateFormat df  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String curTim = df.format(new Date());
        FileWriter fw = new FileWriter(locNode.ip + ".log");
        fw.write(event + ":" + node.ip + curTim + "\n" + "Current Membership List");
        fw.flush();
        fw.close();
    }
}
