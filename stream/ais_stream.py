import socket
import sys,os


try:
    HOST = '153.44.253.27'    # The remote host
    PORT = 5631             # The same port as used by the server
    OUT_PATH = "/home/kentt/nfs_hdfs/DATASETS/AIS/ais_stream.raw"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    while True:
        data = s.recv(1024)
        data = data.split("\n")

        with open(OUT_PATH,'a') as f:
            for line in data:
                f.write("hei"+line+"\n")
    
except KeyboardInterrupt:
    print 'Interrupted'
    s.close()
    f.close()
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
