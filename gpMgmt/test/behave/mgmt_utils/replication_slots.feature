@replication_slots
Feature: Replication Slots

  Scenario: Replication slots are created for a new mirrored cluster
    Given I have a machine with no cluster
    When I create a cluster
    Then the primaries and mirrors should be replicating using replication slots

  Scenario: Replication slots remain correct after failover and rebalance
    Given I have a machine with no cluster
    And I create a cluster

    Given a preferred primary has failed
    When the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And the segments are synchronized
    And primary and mirror switch to non-preferred roles
    Then the primaries and mirrors should be replicating using replication slots
    And the mirrors should not have replication slots

    When the user runs "gprecoverseg -ra"
    Then gprecoverseg should return a return code of 0
    And the segments are synchronized
    And the primaries and mirrors should be replicating using replication slots

  @extended
  Scenario: Replication slots remain correct after full recovery
    Given I have a machine with no cluster
    And I create a cluster

    When a mirror has crashed
    And the user runs "gprecoverseg -aFv"
    And gprecoverseg should return a return code of 0
    And the segments are synchronized
    Then the primaries and mirrors should be replicating using replication slots

  @extended
  Scenario: Replication slots remain correct after expansion
    Given I have a machine with no cluster
    And I create a cluster

    When I add a segment to the cluster
    And the segments are synchronized
    Then the primaries and mirrors should be replicating using replication slots

  Scenario: Replication slots are created when mirrors are added later
    Given I cluster with no mirrors
    When I add mirrors to the cluster
    Then the primaries and mirrors should be replicating using replication slots
