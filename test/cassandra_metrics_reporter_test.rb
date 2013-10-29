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
    @reporter.connection.expects(:execute).at_least_once
    @reporter.connection.expects(:close)
    expected = "INSERT INTO table (server,metric,time,v) VALUES ('server','counter.testing','#{Time.now.utc.strftime("%Y-%m-%d %H:%M:%S+0000")}',1)"
    @reporter.connection.expects(:execute).with(expected)
    #@reporter.connection.expects(:puts).with("put gauge.testing #{Time.now.to_i} 123")
    @reporter.write
  end
  def test_reset

    counter = @registry.counter('counter.testing')
    counter.increment
    counter.reset_on_submit = true
    assert_equal @registry.counter('counter.testing').count, 1
    cassandra_connection = mock
    @reporter.stubs(:connection).returns(cassandra_connection)
    @reporter.connection.expects(:execute).at_least_once
    @reporter.stubs(:open_connection).returns(nil)
    @reporter.connection.expects(:close)
    @reporter.write
    assert_equal @registry.counter('counter.testing').count, 0
  end
end
