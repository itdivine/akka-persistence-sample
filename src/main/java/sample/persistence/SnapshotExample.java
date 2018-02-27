/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package sample.persistence;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.SnapshotOffer;

import java.io.Serializable;
import java.util.ArrayList;

public class SnapshotExample {
  public static class ExampleState implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ArrayList<String> received;

    public ExampleState() {
      this(new ArrayList<String>());
    }

    public ExampleState(ArrayList<String> received) {
      this.received = received;
    }

    public ExampleState copy() {
      return new ExampleState(new ArrayList<String>(received));
    }

    public void update(String s) {
      received.add(s);
    }

    @Override
    public String toString() {
      return received.toString();
    }
  }

  public static class ExamplePersistentActor extends AbstractPersistentActor {
    private ExampleState state = new ExampleState();

    @Override
    public String persistenceId() { return "sample-id-3"; }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .matchEquals("print", s -> System.out.println("current state = " + state))
        .matchEquals("snap", s -> {
          saveSnapshot(state.copy()); // 1.保存快照
        })
        .match(String.class, s -> {
          persist(s, evt -> state.update(evt));  // 2.保存数据源
        })
        .build();
    }

    /**
     * 恢复机制
     * 1.如果只有数据源，按照数据源历史流入的顺序重新恢复
     * 2.如果只有快照，只恢复最近一次快照中的数据
     * 3.如果快照和数据源都有，恢复最近一次快照，和它后面的事件源
     * @return
     */
    @Override
    public Receive createReceiveRecover() {
      return receiveBuilder()
        .match(String.class, evt -> {
          //2.根据事件源恢复状态信息到崩溃前
          System.out.println("offered state = " + evt);
          state.update(evt);
        })
        .match(SnapshotOffer.class, ss -> {
          //1.根据快照恢复最近状态信息
          System.out.println("offered state = " + ss);
          state = (ExampleState) ss.snapshot();
        })
        .build();
    }
  }

  public static void main(String... args) throws Exception {
    final ActorSystem system = ActorSystem.create("example");
    final ActorRef persistentActor = system.actorOf(Props.create(ExamplePersistentActor.class), "persistentActor-3-java");

//    persistentActor.tell("a", null);
//    persistentActor.tell("b", null);
//    persistentActor.tell("snap", null);
//    persistentActor.tell("c", null);
//    persistentActor.tell("d", null);

    persistentActor.tell("print", null);

    Thread.sleep(60000);
    system.terminate();
  }
}
