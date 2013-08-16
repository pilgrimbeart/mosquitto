#! /usr/bin/python
import mosquitto
import socket, os   # Only to get our UID
import time

HOST = "localhost"
SEND_SECS = 1.0/2000
# On my 4-CPU laptop, per 1000 messages/sec:
#    the mosquitto broker uses only about 1% of a CPU
#    this client (doing pub+sub) uses about 10x this (Python overhead?)
# But for reasons I don't quite understand, I can't run broker plus 1 client at more than about 2000 messages/sec
 
g_startTime = 0
g_txs=0
g_rxs=0
g_UID = socket.getfqdn()+";"+str(os.getpid())

def on_connect(mosq, obj, rc):
    if rc == 0:
        print "Connected successfully"
    else:
        print "Problem connecting",rc

def on_disconnect(mosq, obj, rc):
    if rc == 0:
        print "Disconnected successfully"
    else:
        print "Problem disconnecting",rc

def on_subscribe(mosq, obj, mid, qos_list):
    print "Subscribe with mid "+str(mid)+" received. qos_list="+str(qos_list)

def on_unsubscribe(mosq, obj, mid, qos_list):
    print "Unsubscribe with mid "+str(mid)+" received"
    
def on_publish(mosq, obj, mid):
    global g_txs
    g_txs += 1
    if(SEND_SECS > 0.1):
        print "Message "+str(mid)+" published"

def on_message(mosq, obj, msg):
    global g_rxs
    g_rxs += 1
    if(SEND_SECS > 0.1):
        print "Message received: Topic='"+msg.topic+"' payload='"+msg.payload+"' QoS="+str(msg.qos)

def main():
    global g_startTime
    g_startTime = time.time()
    lastSent = time.time()
    
    print "Client",g_UID,"starting"
    client = mosquitto.Mosquitto(g_UID)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe
    client.on_unsubscribe = on_unsubscribe
    client.on_publish = on_publish
    client.on_message = on_message
    client.connect(HOST)
    client.subscribe("my/topic",0)
    #client.subscribe("$SYS/#",0)
   
    client.loop_start()	# Start a separate thread (efficient alternative to calling client.loop() repeatedly in our main loop)

    sends = 0
    startTime = time.time()
    while True:
        # client.loop(0)  # Don't use, as consumes 100% of a CPU
        secsToNext = sends*SEND_SECS - (time.time() - startTime)
        if secsToNext < -1:	
            print secsToNext
            print "Failing to keep up"
        if(secsToNext <= 0):
            lastSent = time.time()
            client.publish("my/topic","hello from "+g_UID,1)
            sends+=1
            # print "Rxs:",g_rxs,"=",g_rxs/(time.time()-g_startTime),"/sec",
            # print "Txs:",g_txs,"=",g_txs/(time.time()-g_startTime),"/sec"
        else:
            time.sleep(secsToNext)
                
    #client.unsubscribe("my/topic")
    #client.disconnect()
    
if __name__ == "__main__":
    main()
