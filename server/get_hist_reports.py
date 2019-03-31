# Created by Markus Kaefer
import socket
import time

UDP_IP = "192.168.83.221"
UDP_PORT = 7090
COMMAND = "report"
i=130

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT

while i > 99:
	MESSAGE = COMMAND + " " + str(i)
	print "message: " + MESSAGE

	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
	sock.sendto(bytes(MESSAGE), (UDP_IP, UDP_PORT))

	time.sleep(0.1)
	i-=1

