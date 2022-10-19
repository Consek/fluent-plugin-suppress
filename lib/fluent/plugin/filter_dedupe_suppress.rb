require "fluent/plugin/filter"

module Fluent::Plugin
  class DedupeSuppressFilter < Fluent::Plugin::Filter
    Fluent::Plugin.register_filter("dedupe-suppress", self)

    config_param :attr_keys, :string, default: nil
    config_param :num, :integer, default: 3
    config_param :max_slot_num, :integer, default: 100000
    config_param :interval, :integer, default: 300
    config_param :supress_info_field, :string, default: "supressed_dates"

    def configure(conf)
      super
      @keys = @attr_keys ? @attr_keys.split(/ *, */) : nil
      @slots = {}
    end

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new
      es.each do |time, record|
        if @keys
          keys = @keys.map do |key|
            key.split(/\./).inject(record) { |r, k| r[k] }
          end
          key = tag + "\0" + keys.join("\0")
        else
          key = tag
        end
        slot = @slots[key] ||= []

        expired = time.to_f - @interval
        if (slot.first && (slot.first <= expired)) || slot.length > @num
          slot.shift
          unless slot.empty?
            record[@supress_info_field] = slot.map { |item| Time.at(item).utc.strftime("%Y-%m-%d %H:%M:%S.%3N %z") }
            slot.clear
          end
        elsif slot.first
          slot.push(time.to_f)
          next
        else
          slot.push(time.to_f)
        end

        if @slots.length > @max_slot_num
          (evict_key, evict_slot) = @slots.shift
          if evict_slot.last && (evict_slot.last > expired)
            log.warn "@slots length exceeded @max_slot_num: #{@max_slot_num}. Evicted slot for the key: #{evict_key}"
          end
        end

        new_es.add(time, record)
      end
      return new_es
    end
  end
end
