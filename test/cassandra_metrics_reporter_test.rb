require 'test_helper'
require 'thread_error_handling_tests'
require 'metriks/reporter/cassandra'

class CassandraReporterTest < Test::Unit::TestCase
  include ThreadErrorHandlingTests

  def build_reporter(options={})
    Metriks::Reporter::Cassandra.new('localhost', { :registry => @registry, :db => "metrics", :source => "server", :table => "table" }.merge(options))
  end

  def setup
    @registry = Metriks::Registry.new
    @reporter = build_reporter
  end

  def teardown
    @reporter.stop
    @registry.stop
  end

  def test_write
    @registry.meter('meter.testing').mark
    @registry.counter('counter.testing').increment
    @registry.timer('timer.testing').update(1.5)
    @registry.histogram('histogram.testing').update(1.5)
    @registry.utilization_timer('utilization_timer.testing').update(1.5)
    @registry.gauge('gauge.testing') { 123 }
    cassandra_connection = mock
    @reporter.stubs(:connection).returns(cassandra_connection)
    @reporter.stubs(:open_connection).returns(nil)
    @reporter.connection.expects(:close)
    expected = "INSERT INTO table (server,metric,time,v) VALUES ('server','counter.testing','#{Time.now.utc.strftime("%Y-%m-%d %H:%M:%S+0000")}',1)"
    @reporter.expects(:execute_prepared_statement).at_least_once
    @reporter.expects(:execute_prepared_statement).with(['server', 'meter.testing.one_minute_rate', "#{Time.now.to_i}", 0])
    @reporter.expects(:execute_prepared_statement).with(["server","timer.testing.max","#{Time.now.to_i}",1.5])
    @reporter.write
  end
  def test_counter_reset

    counter = @registry.counter('counter.testing')
    counter.increment
    counter.reset_on_submit = true
    assert_equal @registry.counter('counter.testing').count, 1
    cassandra_connection = mock
    @reporter.stubs(:connection).returns(cassandra_connection)
    @reporter.stubs(:open_connection).returns(nil)
    @reporter.connection.expects(:close)
    @reporter.expects(:execute_prepared_statement).with(['server', 'counter.testing.count', "#{Time.now.to_i}", 1])
    @reporter.write
    assert_equal @registry.counter('counter.testing').count, 0
  end
  def test_timer_reset

    timer = @registry.timer('timer.testing')
    timer.update(10)
    timer.reset_on_submit = true
    assert_equal @registry.timer('timer.testing').max, 10
    cassandra_connection = mock
    @reporter.stubs(:connection).returns(cassandra_connection)
    @reporter.stubs(:open_connection).returns(nil)
    @reporter.connection.expects(:close)
    @reporter.expects(:execute_prepared_statement).at_least_once
    @reporter.expects(:execute_prepared_statement).with(['server', 'timer.testing.max', "#{Time.now.to_i}", 10])
    @reporter.write
    assert_equal @registry.timer('timer.testing').max, 0
  end

end
