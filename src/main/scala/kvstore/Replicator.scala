package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._

object Replicator {

    case class Replicate(key: String, valueOption: Option[String], id: Long)

    case class Replicated(key: String, id: Long)

    case class Snapshot(key: String, valueOption: Option[String], seq: Long)

    case class SnapshotAck(key: String, seq: Long)

    def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

    import Replicator._
    import Replica._
    import context.dispatcher

    /*
     * The contents of this actor is just a suggestion, you can implement it in any way you like.
     */

    // map from sequence number to pair of sender and request
    var acks = Map.empty[Long, (ActorRef, Replicate)]
    // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
    var pending = Vector.empty[Snapshot]

    var seqToSnapOp: Map[Long, Cancellable] = Map.empty[Long, Cancellable]

    var _seqCounter = 0L

    def nextSeq = {
        val ret = _seqCounter
        _seqCounter += 1
        ret
    }


    /* TODO Behavior for the Replicator. */
    def receive: Receive = {
        case rep@Replicate(key, valueOpt, id) =>
        {
            val snapshot = Snapshot(key, valueOpt, _seqCounter)
            acks += _seqCounter -> (sender, rep)
            val op = context.system.scheduler.schedule(0 millis, 100 millis){replica ! snapshot}
            seqToSnapOp +=_seqCounter -> op
        }
        case SnapshotAck(key, seq) =>
        {
            seqToSnapOp.get(seq).map(_.cancel())
            acks.get(seq).map{case (req, rep) => req ! Replicated(rep.key, rep.id)}
            nextSeq
        }
        case _ => println("Replicator receive - unknown message!")
    }

}
