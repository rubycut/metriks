require 'net/https'
require 'socket'
require 'io/wait'
require 'cql'

module Metriks::Reporter
  class Cassandra
    attr_accessor :prefix, :source

    def initialize(host, options = {})
      @host = host
      @port = options[:port] || "9042"
      @prefix = options[:prefix]
      @source = options[:source]
      @database = options[:database]
      @table = options[:table]
      @interval = options[:interval] || 60
      @registry  = options[:registry] || Metriks::Registry.default
      @on_error  = options[:on_error] || proc { |ex| }

    end
    def open_connection
      @connection = Cql::Client.connect(host: @host, port: @port)
      @connection.use(@database)
      @connection
      @write_statement||= @connection.prepare("INSERT INTO #{@table}(server,metric, time, v) VALUES
          (?, ?, ?, ?) USING TTL 1209600") # two weeks
    end
    def connection
      @connection
    end
    def start
      @thread ||= Thread.new do
        loop do
          sleep @interval
          Thread.new do
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
      open_connection
      @registry.each do |name, metric|
        case metric
        when Metriks::Meter
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate
          ]
        when Metriks::Counter
          send_metric name, metric, [
            :count
          ]
          metric.clear if metric.reset_on_submit
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
      connection.close
    end
    def send_metric(compound_name, metric, keys, snapshot_keys = [])
      keys.each do |key|
        execute_preparated_statement ['#{@source}','#{compound_name}','#{Time.now.to_i}',#{metric.send(key)})"
        connection.execute command
      end
    end
    def execute_prepared_statement array
      log.debug "I plan to update cassandra metrics with:\n#{array.inspect}"
      future = write_statement.async.execute(*array)
      future.on_failure do |error|
        log.error "Writing prepared statement error: '#{error.message}' while executing: #{error.cql}"
      end
    end
    def write_statement
      @write_statement
    end

  end
end
