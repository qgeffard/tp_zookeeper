package org.qgeff.amd.tp_zookeeper;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away.
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKClient implements Watcher {
	private static Logger logger = LoggerFactory.getLogger(ZKClient.class);
	ZooKeeper zk;

	/**
	 * Demarre le client zookeeper sur le serveur en param
	 * @param hostPort
	 * @throws KeeperException
	 * @throws IOException
	 */
	public ZKClient(String hostPort) throws KeeperException, IOException {
		zk = new ZooKeeper(hostPort, 3000, this);
	}
	
	public void createZNode(String path){
		try {
			zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public boolean insertStringValue(String path, String value) {
		try {
			Stat stat = zk.setData(path, value.getBytes(), zk.exists(path, true).getVersion());
			return true;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public String readStringValue(String path) {
		
		byte[] data = new String("").getBytes();
		
		try {
			//get data of "/" path, false so no watcher left, and null stat given
			data = zk.getData(path, true, null);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			return new String(data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		};
		
		return "";
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws KeeperException, IOException, InterruptedException {
		
		String hostPort = args.length ==  0 ? "localhost:2181" : args[0];
		ZKClient client = new ZKClient(hostPort);
		
		ZKClient.logger.debug("zookeeper client connected, wait 2 sec");
		Thread.sleep(2000);
		
		ZKClient.logger.debug("create zookeeper znode ..");
		client.createZNode("/tpzk");
		
		ZKClient.logger.debug("set FOO data on \"/tpzk\" path");
		client.insertStringValue("/tpzk", "FOO");
		
		ZKClient.logger.debug(" wait 2 sec again");
		Thread.sleep(2000);
		
		String value = client.readStringValue("/tpzk");
		ZKClient.logger.debug("value readed : {}", value);
	}

	public void process(WatchedEvent event) {
	}
}