require 'atomic'
require 'hitimes'

require 'metriks/meter'
require 'metriks/histogram'

module Metriks
  class Timer
    # I want to get mean value for last minute only not for eternity
    attr_accessor :reset_on_submit
    class Context
      def initialize(timer)
        @timer    = timer
        @interval = Hitimes::Interval.now
      end

      def restart
        @interval = Hitimes::Interval.now
      end

      def stop
        @interval.stop
        @timer.update(@interval.duration)
      end
    end

    def initialize(histogram = Metriks::Histogram.new_exponentially_decaying)
      @meter     = Metriks::Meter.new
      @histogram = histogram
    end
    # shortcut for onliners
    def activate_reset_on_submit
      @reset_on_submit = true
      self
    end

    def clear
      @meter.clear
      @histogram.clear
    end

    def update(duration)
      if duration >= 0
        @meter.mark
        @histogram.update(duration)
      end
    end

    def time(callable = nil, &block)
      callable ||= block
      context = Context.new(self)

      if callable.nil?
        return context
      end

      begin
        return callable.call
      ensure
        context.stop
      end
    end

    def snapshot
      @histogram.snapshot
    end

    def count
      @histogram.count
    end

    def one_minute_rate
      @meter.one_minute_rate
    end

    def five_minute_rate
      @meter.five_minute_rate
    end

    def fifteen_minute_rate
      @meter.fifteen_minute_rate
    end

    def mean_rate
      @meter.mean_rate
    end

    def min
      @histogram.min
    end

    def max
      @histogram.max
    end

    def mean
      @histogram.mean
    end

    def stddev
      @histogram.stddev
    end

    def stop
      @meter.stop
    end
  end
end