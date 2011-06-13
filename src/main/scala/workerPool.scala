/*
 * Workers pool class for example applications.
 *
 * @Author:     Giovanni Ruggiero
 * @Email:      giovanni.ruggiero@gmail.com
*/

import scala.collection.mutable.ListMap

class WorkerPool extends ListMap[Array[Byte], Long] {
	private val MaxWorkers          = 100
	private val HeartbeatLiveness   = 3       //  3-5 is reasonable
	private val HeartbeatInterval   = 1000    //  msecs

	def enqueue (address: Array[Byte]) = {
		assert(this.size < MaxWorkers)
		this(address) = System.currentTimeMillis + HeartbeatLiveness * HeartbeatInterval
	}

	def dequeue () = {
		val worker = this.head._1
		this -= worker
		worker
	}

	def purge () = {
		this.retain{(_, refr) => refr > System.currentTimeMillis}
	}

}
