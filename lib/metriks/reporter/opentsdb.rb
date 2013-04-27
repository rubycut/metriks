require 'net/https'
require 'socket'
require 'io/wait'

module Metriks::Reporter
  class Opentsdb
    attr_accessor :prefix, :source

    def initialize(host, port, options = {})
      @host = host
      @port = port
      @prefix = options[:prefix]
      @source = options[:source]
      @interval = options[:interval] || 60
      @registry  = options[:registry] || Metriks::Registry.default
      @on_error  = options[:on_error] || proc { |ex| }

    end
    def connection
      @connection ||= TCPSocket.new(@host, @port)
    end
    def start
      @thread ||= Thread.new do
        loop do
          Thread.new do
            sleep @interval
            begin
              write
            rescue Exception => ex
              @on_error[ex] rescue nil
            end
          end
        end
      end
    end

    def stop
      @thread.kill if @thread
      @thread = nil
    end

    def restart
      stop
      start
    end

    def write
      gauges = []
      @registry.each do |name, metric|
        gauges << case metric
        when Metriks::Meter
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate
          ]
        when Metriks::Counter
          send_metric name, metric, [
            :count
          ]
        when Metriks::Gauge
          send_metric name, metric, [
            :value
          ]
        when Metriks::UtilizationTimer
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate,
            :min, :max, :mean, :stddev,
            :one_minute_utilization, :five_minute_utilization,
            :fifteen_minute_utilization, :mean_utilization,
          ], [
            :median, :get_95th_percentile
          ]
        when Metriks::Timer
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate,
            :min, :max, :mean, :stddev
          ], [
            :median, :get_95th_percentile
          ]
        when Metriks::Histogram
          send_metric name, metric, [
            :count, :min, :max, :mean, :stddev
          ], [
            :median, :get_95th_percentile
          ]
        end
      end
      close_connection

    end
    def close_connection
      if connection.ready?
       puts "Errors"
       #line = connection.gets # Read lines from socket
      else
      puts "everything ok"
      end
      connection.close
    end
    def send_metric(base_name, metric, keys, snapshot_keys = [])
      keys.each do |key|
        connection.puts("put #{base_name}.#{key} #{Time.now.to_i} #{metric.send(key)} host=desktopstats")
      end
    end
  end
end
