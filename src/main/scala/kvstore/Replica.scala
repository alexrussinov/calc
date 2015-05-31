package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {

    sealed trait Operation {
        def key: String

        def id: Long
    }

    case class Insert(key: String, value: String, id: Long) extends Operation

    case class Remove(key: String, id: Long) extends Operation

    case class Get(key: String, id: Long) extends Operation

    sealed trait OperationReply

    case class OperationAck(id: Long) extends OperationReply

    case class OperationFailed(id: Long) extends OperationReply

    case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

    def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

    import Replica._
    import Replicator._
    import Persistence._
    import context.dispatcher

    /*
     * The contents of this actor is just a suggestion, you can implement it in any way you like.
     */

    var updateOp: Map[Long, Cancellable] = Map.empty[Long, Cancellable]

    var operations: Map[Long, (String, Option[String])] = Map.empty[Long, (String, Option[String])]

    var operationRequester: Map[Long, ActorRef] = Map.empty[Long, ActorRef]

    var expectedSeq: Long = 0L

    var seqToReplicator: Map[Long, ActorRef] = Map.empty[Long, ActorRef]

    var kv = Map.empty[String, String]
    // a map from secondary replicas to replicators
    var secondaries = Map.empty[ActorRef, ActorRef]
    // the current set of replicators
    var replicators = Set.empty[ActorRef]

    val persistence = context.system.actorOf(persistenceProps)

    // register self in a Arbiter
    arbiter ! Join

    def receive = {
        case JoinedPrimary => context.become(leader)
        case JoinedSecondary => context.become(replica)
    }

    /* TODO Behavior for  the leader role. */
    val leader: Receive = {
        case ins@Insert(key, value, id) =>
        {
            opProcessing(id, key, Some(value), sender)
        }
        case rm@Remove(key, id) =>
        {
            opProcessing(id, key, None, sender)
        }
        case get@Get(key, id) =>
        {
            sender ! GetResult(key, kv.get(key), id)
        }
        case Persisted(key, id) =>
        {
            acknowledgement(id)
        }
        case _ =>
    }

    def acknowledgement(id: Long) =
    {
        println("ackn id: " + id)
        updateOp.get(id).map{c =>
            c.cancel()
            operationRequester.get(id).map( _ ! OperationAck(id))
            operationRequester -= id
            operations.get(id) match
            {
                case Some((key, Some(v))) =>
                    kv += key -> v
                case Some((key, None)) =>
                    kv -= key
                case None => println("Oooops, operations is empty!")
            }
        }
    }

    def opProcessing(id: Long, key: String, value: Option[String], requester: ActorRef) =
    {
        operationRequester += id -> requester
        operations += id -> (key, value)
        persistence ! Persist(key, value, id)
        val op = context.system.scheduler.scheduleOnce(1 second){
            sender ! OperationFailed(id)
            updateOp -= id
            operations -= id
        }
        updateOp += id -> op
    }

    /* TODO Behavior for the replica role. */
    val replica: Receive = {
        case get@Get(key, id) =>
        {
            sender ! GetResult(key, kv.get(key), id)
        }
        case Snapshot(key, valueOpt, seq) =>
        {
            if(seq < expectedSeq){
                println(s" seq: $seq < exp: $expectedSeq")
                sender ! SnapshotAck(key, seq)
            }
            else if(seq == expectedSeq)
            {
                seqToReplicator += seq -> sender
                opProcessing(seq, key, valueOpt, self)
            }
        }
        case Persisted(key, id) =>
        {
            acknowledgement(id)
        }
        case OperationAck(id) =>
        {
            operations.get(id).map{ case(k: String, v: Option[String]) => seqToReplicator.get(id).map(_ ! SnapshotAck(k, id))}
            if(expectedSeq < id +1) expectedSeq += 1
        }
        case OperationFailed(id) =>
        {
            println(s"Secondary replica op: $id persistent failed!")
        }
        case _ =>
    }

}

