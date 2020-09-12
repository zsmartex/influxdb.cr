require "./query/*"

module InfluxDB
  class Query
    include Enumerable(Result)

    property! fields : String
    property! measurement : String

    def initialize(@client : Client, @db : String)
      @measurement = db
      @results = [] of Result
    end

    def select(fields = "*")
      @fields = fields
      self
    end

    def from(measurement : String)
      @measurement = measurement
      self
    end

    def each
      execute
      @results.each { |pv| yield pv }
    end

    def execute
      self.parse_results @client.query(build_query, db: @db)
    end

    private def build_query
      String.build do |str|
        str << "SELECT #{fields} FROM #{measurement}"
      end
    end

    def self.parse_results(results)
      rq = [] of Result
      results[0]["series"].as_a.each do |series|
        name = series["name"].as_s
        columns = series["columns"]
        series["values"].as_a.each do |value|
          fields = Fields.new
          i = 0
          value.as_a.each do |v|
            fields[columns[i].as_s] = v.to_s
            i += 1
          end
          rq << Result.new(name, fields)
        end
      end
      rq
    end
  end
end
