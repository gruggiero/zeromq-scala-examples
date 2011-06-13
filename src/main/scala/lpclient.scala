/*
 *  Lazy Pirate client
 *  Use zmq_poll to do a safe request-reply
 *  To run, start lpserver and then randomly kill/restart it
 *
 *
 *  @Author:     Giovanni Ruggiero
 *  @Email:      giovanni.ruggiero@gmail.com
 */

import util.control.Breaks._
import org.zeromq.ZMQ
import ZHelpers._

object lpclient {
	val ZmqPollMsec = 1000
	val RequestTimeout = 2500    //  msecs, (> 1000!)
	val RequestRetries = 3       //  Before we abandon
	val ServerEndpoint = "tcp://localhost:5555"

	def main(args : Array[String]) {
		val ctx = ZMQ.context(1)
		printf ("I: connecting to server...\n")
		var client = ctx.socket(ZMQ.REQ)
		client.connect(ServerEndpoint)

		var sequence = 0
		var retriesLeft = RequestRetries
		breakable {
			while (retriesLeft > 0 && !Thread.currentThread().isInterrupted) {
				//  We send a request, then we work to get a reply
				sequence += 1
				val request = String.format("%d", sequence: Integer)
				client.send(request.getBytes(), 0)
				var expectReply = true
				while (expectReply) {
					//  Poll socket for a reply, with timeout
					val items = ctx.poller(1)
					items.register(client,ZMQ.Poller.POLLIN)
					items.poll(RequestTimeout * ZmqPollMsec)
					if(items.pollin(0)) {
						//  We got a reply from the server, must match sequence
						val reply = new String(client.recv(0))
						if (reply.toInt == sequence) {
							printf ("I: server replied OK (%s)\n", reply)
							retriesLeft = RequestRetries
							expectReply = false
						} else {
							printf ("E: malformed reply from server: %s\n", reply)
						}
					} else {
						retriesLeft -= 1
						if (retriesLeft == 0) {
							println ("E: server seems to be offline, abandoning")
							break
						} else {
							println ("W: no response from server, retrying...")
							//  Old socket is confused; close it and open a new one
							client.close
							println ("I: reconnecting to server...")
							client = ctx.socket(ZMQ.REQ)
							client.connect(ServerEndpoint)
							//  Send request again, on new socket
							client.send(request.getBytes(), 0)
						}
					}
				}
			}
		}
	}
}
