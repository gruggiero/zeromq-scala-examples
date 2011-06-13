/*
 *  Paranoid Pirate queue
 *
 *  @Author:     Giovanni Ruggiero
 *  @Email:      giovanni.ruggiero@gmail.com
 */

import org.zeromq.ZMQ
import ZHelpers._

object ppqueue {
	val Ready     = "\001" getBytes
	val Heartbeat = "\002" getBytes

	val HeartbeatLiveness   = 3       //  3-5 is reasonable
	val HeartbeatInterval   = 1000    //  msecs

  def main(args : Array[String]) {
		val NOFLAGS = 0

		//  Prepare our context and sockets
		val ctx = ZMQ.context(1)
		val frontend = ctx.socket(ZMQ.ROUTER)
		val backend = ctx.socket(ZMQ.ROUTER)

		frontend.bind("tcp://*:5555")
		backend.bind("tcp://*:5556")

		// Send out heartbeats at regular intervals
		var heartbeatTime = System.currentTimeMillis + HeartbeatInterval

		val workerQueue = scala.collection.mutable.Queue[Array[Byte]]()
		val workers = new WorkerPool

		val poller = ctx.poller(2)

		poller.register(backend,ZMQ.Poller.POLLIN)
		poller.register(frontend,ZMQ.Poller.POLLIN)

		while (true) {
			poller.poll

			if(poller.pollin(0)) {
				val msg = new ZMsg(backend)
				// msg.dump
				val workerAddr = msg.unwrap
				println(new String(workerAddr))
				//  Queue worker address for LRU routing
				workerQueue.enqueue(workerAddr)
				//  Address is READY or else a client reply address
				val clientAddr = msg.address
				if (!clientAddr.equals(Ready)) {
					frontend.sendMsg(msg)
				}
			}
			if(poller.pollin(1)) {
				//  Now get next client request, route to next worker
				val msg = new ZMsg(frontend)
				msg.dump
				msg.wrap(workerQueue.dequeue)
				msg.dump
				backend.sendMsg(msg)
			}
			//  Send heartbeats to idle workers if it's time
      // if (System.currentTimeMillis >= heartbeatTime) {
			// 	workers foreach { worker =>
			// 		val msg = new ZMsg(Heartbeat)
			// 		msg.wrap(worker._1)
			// 		backend.sendMsg(msg)
			// 	}
			// 	heartbeatTime = System.currentTimeMillis + HeartbeatInterval
      // }
		}
	}
}
