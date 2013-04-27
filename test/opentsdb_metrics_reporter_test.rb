require 'test_helper'
require 'thread_error_handling_tests'

require 'metriks/reporter/opentsdb'

class OpentsdbReporterTest < Test::Unit::TestCase
  include ThreadErrorHandlingTests

  def build_reporter(options={})
    Metriks::Reporter::Opentsdb.new('localhost', 4242, { :registry => @registry }.merge(options))
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
    tcp_socket = mock
    @reporter.stubs(:connection).returns(tcp_socket)
    tcp_socket.expects(:puts).at_least_once
    tcp_socket.expects(:ready?).returns(false)
    tcp_socket.expects(:close)

    @reporter.connection.expects(:puts).with("put counter.testing.count #{Time.now.to_i} 1 host=desktopstats")
    @reporter.write
  end
end
