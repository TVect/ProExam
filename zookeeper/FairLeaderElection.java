package com.dataguru.kafka05;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

public class FairLeaderElection {
	
    private static final String CONNECTION_STRING = "192.168.0.100:2181,192.168.0.100:2181,192.168.168.0.102:2181";
    private static final String GROUP_PATH = "/leader_base/leader_group";
    private static final int SESSION_TIMEOUT = 10000;
    
    private ZooKeeper zk = null;
    private String selfPath = null;
    
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    
	public void registerNode(String data) throws Exception{
        zk = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				System.out.println("Create Node successfully");
				connectedSemaphore.countDown();
			}
        	
        });
        connectedSemaphore.await();
        this.selfPath = zk.create(GROUP_PATH, data.getBytes(),
        		ZooDefs.Ids.OPEN_ACL_UNSAFE, 
        		CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
    
	/*
	 * 检测自己是不是最小的节点
	 */
    public boolean checkMinPath(LeaderCallback leaderCall) throws KeeperException, InterruptedException {
        List<String> subNodes = zk.getChildren(GROUP_PATH, false);
        Collections.sort(subNodes);
        int index = subNodes.indexOf(this.selfPath.substring(GROUP_PATH.length()+1));
        switch (index){
            case -1:{
                System.out.println("Node not exists..."+selfPath);
                return false;
            }
            case 0:{
            	System.out.println("I am the leader..."+selfPath);
            	leaderCall.leaderEvent("");
                return true;
            }
            default:{
                String waitPath = GROUP_PATH +"/"+ subNodes.get(index - 1);
                System.out.println("waitPath: "+waitPath);
                try{
                    zk.getData(waitPath, new Watcher(){

						@Override
						public void process(WatchedEvent event) {
					        if(event == null){
					            return;
					        }
					        Event.KeeperState keeperState = event.getState();
					        Event.EventType eventType = event.getType();
					        if (event.getType() == Event.EventType.NodeDeleted) {
					        	System.out.println("Wait Path id deleted");
				                try {
				                    checkMinPath(leaderCall);
				                } catch (KeeperException e) {
				                    e.printStackTrace();
				                } catch (InterruptedException e) {
				                    e.printStackTrace();
				                }
				            }else {
				            	System.out.println("event_type:"+eventType);
				            }
						}
                    	
                    }, new Stat());
                    return false;
                }catch(KeeperException e){
                    if(zk.exists(waitPath,false) == null){
                        System.out.println("子节点中，排在我前面的"+waitPath+"已丢失");
                        return checkMinPath(leaderCall);
                    }else{
                        throw e;
                    }
                }
            }
                
        }
    }    
    
    
    /*
     * LeaderShip支持回调
     */
    public Boolean getLeaderShipInBack(LeaderCallback leaderCallback) throws KeeperException, InterruptedException{
    	return checkMinPath(leaderCallback);
    }
    
    
    /*
     * 阻塞直到获取LeaderShip
     */
    public void getLeaderShip() throws KeeperException, InterruptedException{
    	CountDownLatch leaderShipSemaphore = new CountDownLatch(1);
    	checkMinPath(new LeaderCallback(){
			public void leaderEvent(String event) {
				leaderShipSemaphore.countDown();
			}
    	});
    	
    	leaderShipSemaphore.await();
    	return;
    }
    
    
	public static void main(String[] args) {
	
	}
}


interface LeaderCallback {
    public void leaderEvent(String event);
}