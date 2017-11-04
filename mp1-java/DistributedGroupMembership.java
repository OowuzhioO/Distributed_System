package uiuc.mp2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by haowenjiang on 10/5/17.
 */
public class DistributedGroupMembership {

    //Distinguish the machine is Introducer or Follower
    public static void main (String[] args) throws IOException {
        if (args[0].equals("introducer")) {
            ProgressStart progressStart = new ProgressStart(true);
            progressStart.start();
        } else {
            ProgressStart progressStart = new ProgressStart(false);
            progressStart.start();
        }
    }
}
