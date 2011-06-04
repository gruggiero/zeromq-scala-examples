/*
 *  Simple Pirate queue
 *  This is identical to the LRU pattern, with no reliability mechanisms
 *  at all. It depends on the client for recovery. Runs forever.
 *
 *  Author:     Giovanni Ruggiero
 *  Email:      giovanni.ruggiero@gmail.com
 */

import org.zeromq.ZMQ
import ZHelpers._

object spqueue   {
  def main(args : Array[String]) {
		val NOFLAGS = 0

		//  Prepare our context and sockets
		val ctx = ZMQ.context(1)
		val frontend = ctx.socket(ZMQ.ROUTER)
		val backend = ctx.socket(ZMQ.ROUTER)

		frontend.bind("tcp://*:5555")
		backend.bind("tcp://*:5556")

		val workerQueue = scala.collection.mutable.Queue[Array[Byte]]()

		val poller = ctx.poller(2)

		poller.register(backend,ZMQ.Poller.POLLIN)
		poller.register(frontend,ZMQ.Poller.POLLIN)

		while (true) {
			poller.poll

			if(poller.pollin(0)) {
				val msg = new ZMsg(backend)
				val workerAddr = msg.unwrap

				//  Queue worker address for LRU routing
				workerQueue.enqueue(workerAddr)
				//  Address is READY or else a client reply address
				val clientAddr = msg.address
				if (!new String(clientAddr).equals("READY")) {
					frontend.sendMsg(msg)
				}
			}
			if(poller.pollin(1)) {
 				//  Now get next client request, route to LRU worker
				val msg = new ZMsg(frontend)
				msg.wrap(workerQueue.dequeue)
				backend.sendMsg(msg)
			}
		}
	}
}
