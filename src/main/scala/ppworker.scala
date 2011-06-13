/*
 *  Paranoid Pirate worker
 *
 *  @Author:     Giovanni Ruggiero
 *  @Email:      giovanni.ruggiero@gmail.com
 */

import org.zeromq.ZMQ
import ZHelpers._

object ppworker {
	val Ready     = "\001" getBytes
	val Heartbeat = "\002" getBytes

	val HeartbeatLiveness   = 3       //  3-5 is reasonable
	val HeartbeatInterval   = 1000    //  msecs
	val IntervalInit        = 1000    //  Initial reconnect
	val IntervalMax         = 32000   //  After exponential backoff

	//  Helper function that returns a new configured socket
	//  connected to the Paranoid Pirate queue

	def sWorkerSocket(ctx: ZMQ.Context) = {
		val worker = ctx.socket(ZMQ.DEALER)
		setID(worker)
		worker.connect("tcp://localhost:5556")
    val identity = new String(worker getIdentity)
    printf ("I: (%s) worker ready\n", identity)
		// worker.send("READY".getBytes, 0);
		worker.send(Ready, 0)
		worker
	}

	def main(args : Array[String]) {
		val rand = new java.util.Random(System.currentTimeMillis)
		val ctx = ZMQ.context(1)
		var worker = sWorkerSocket(ctx)
		// Send out heartbeats at regular intervals
		var heartbeatTime = System.currentTimeMillis + HeartbeatInterval

		val poller = ctx.poller(1)

		poller.register(worker,ZMQ.Poller.POLLIN)

		var cycles = 0
		var stopped = false
		var liveness = HeartbeatLiveness
		var interval = IntervalInit

		do {
			val msg = worker recvMsg

			println("in cycle")
			if(poller.pollin(0)) {
				//  Get message
				//  - 3-part envelope + content -> request
				//  - 1-part HEARTBEAT -> heartbeat
				val msg = new ZMsg(worker)
				printf("Received %s : %s\n", identity, msg.bodyToString)
				if (msg.size == 3) {
					cycles += 1
					//  Simulate various problems, after a few cycles
					if (cycles > 3 && rand.nextInt() % 5 == 0) {
						printf ("I: (%s) simulating a crash\n", identity)
						stopped = true
					} else if (cycles > 3 && rand.nextInt() % 5 != 0) {
						printf ("I: (%s) simulating CPU overload\n", identity)
						Thread.sleep (3000)
					}
					printf ("I: (%s) normal reply\n", identity);
					worker.sendMsg(msg)
					liveness = HeartbeatLiveness
					Thread.sleep (1000)  //  Do some heavy work
				} else if (msg.size == 1 && msg.bodyToString == Heartbeat) {
					liveness = HeartbeatLiveness
				} else {
					printf ("E: (%s) invalid message\n", identity)
					msg.dump
				}
			} else {
				liveness -= 1
				if (liveness == 0) {
          println ("W: heartbeat failure, can't reach queue")
          printf ("W: reconnecting in (%s) msec...\n", interval)
          Thread.sleep (interval)

          // if (interval < IntervalMax)
            // interval *= 2

					poller.unregister(worker)
					worker = sWorkerSocket(ctx)
					poller.register(worker,ZMQ.Poller.POLLIN)

					liveness = HeartbeatLiveness
				}
       //  Send heartbeat to queue if it's time
				if (System.currentTimeMillis >= heartbeatTime) {
					heartbeatTime = System.currentTimeMillis + HeartbeatInterval
					println ("I: worker heartbeat\n")
					val msg = new ZMsg(Heartbeat)
					worker.sendMsg(msg)
				}
			}
		} while (!stopped)
	}
}
