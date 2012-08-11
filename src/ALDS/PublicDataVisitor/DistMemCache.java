/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.PublicDataVisitor;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

/**
 *
 * @author daidong
 */
public class DistMemCache {

    String host[] = new String[1];
    SockIOPool pool = null;
    MemCachedClient client = null;
    
    public DistMemCache() {

        //host[0] = "219.219.216.40:11211";
        host[0] = "127.0.0.1:11211";
    
        pool = SockIOPool.getInstance();
        pool.setServers(host);
        pool.setFailback(true);
        pool.setInitConn(10);
        pool.setMinConn(5);
        pool.setMaxIdle(1000 * 60 * 60 * 24);
        pool.setMaxConn(250);
        pool.setMaintSleep(30);
        pool.setNagle(false);
        pool.setSocketTO(3000);
        pool.setAliveCheck(true);
        pool.initialize();

        client = new MemCachedClient();

    }
    
    public Object get(String key){
        return client.get(key);
    }
    
    public boolean set(String key, Object value){
        if (client.keyExists(key))
            client.delete(key);
        return client.set(key, value);
    }
    
    public boolean exist(String key) {
        return client.keyExists(key);
    }
    
}