module InfluxDB
  struct Database
    getter :name

    # @@processed = Channel(HTTP::Client::Response)
    # spawn do
    #   loop do
    #     resp = @@processed.receive
    #   end
    # end

    def initialize(@client : Client, @name : String)
      @mutex = Mutex.new
    end

    def query(q)
      @client.query(q, db: name)
    end

    # https://docs.influxdata.com/influxdb/latest/query_language/database_management/#delete-a-database-with-drop-database
    def drop
      query "DROP DATABASE #{name}"
      true
    end

    def select(fields = "*")
      Query.new(@client, name).select(fields)
    end

    def write_point(point_value : PointValue, flush : String?)
      body = String.build { |str|
        str << point_value.to_s(str)
      }.strip

      send_write(body, flush)
    end

    def write_point(series : String, value : Value, tags = Tags.new, timestamp : Time? = nil)
      write_point series, Fields{:value => value}, tags: tags, timestamp: timestamp
    end

    def write_point(series : String, fields : Fields, tags = Tags.new, timestamp : Time? = nil)
      timestamp = Time.utc if timestamp.nil?
      write_point PointValue.new(series, tags: tags, fields: fields, timestamp: timestamp)
    end

    private def send_write(body, flush : String? = "ms")
      @mutex.synchronize do
        @client.post "/write?db=#{name}&precision=#{flush}",
          HTTP::Headers{
            "Content-Type" => "application/octet-stream",
          },
          body
      end
    end

    def write
      pw = PointsWriter.new
      yield pw
      write pw.points
    end
  end
end
