require 'test_helper'
require 'thread_error_handling_tests'
require 'ruby-debug'
require 'metriks/reporter/cassandra'

class CassandraReporterTest < Test::Unit::TestCase
  include ThreadErrorHandlingTests

  def build_reporter(options={})
    Metriks::Reporter::Cassandra.new('localhost', 4242, { :registry => @registry, :db => "metrics" }.merge(options))
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
    @registry.meter('meter.testing#tag=test').mark
    @registry.counter('counter.testing#tag=test').increment
    @registry.timer('timer.testing#tag=test').update(1.5)
    @registry.histogram('histogram.testing#tag=test').update(1.5)
    @registry.utilization_timer('utilization_timer.testing#tag=test').update(1.5)
    @registry.gauge('gauge.testing#tag=test') { 123 }
    cassandra_connection = mock
    @reporter.stubs(:connection).returns(cassandra_connection)
    @reporter.stubs(:open_connection).returns(nil)
    tcp_socket.expects(:execute).at_least_once
    tcp_socket.expects(:close)

    @reporter.connection.expects(:execute).with("insert into put counter.testing #{Time.now.to_i} 1 tag=test")
    @reporter.connection.expects(:puts).with("put gauge.testing #{Time.now.to_i} 123 tag=test")
    @reporter.write
  end
  def test_reset

    counter = @registry.counter('counter.testing#tag=test')
    counter.increment
    counter.reset_on_submit = true
    assert_equal @registry.counter('counter.testing#tag=test').count, 1
    tcp_socket = mock
    @reporter.stubs(:connection).returns(tcp_socket)
    tcp_socket.expects(:puts).at_least_once
    tcp_socket.expects(:ready?).returns(false)
    tcp_socket.expects(:close)

    @reporter.stubs(:open_connection).returns(nil)
    @reporter.write
    assert_equal @registry.counter('counter.testing#tag=test').count, 0
  end
end
