load File.dirname(__FILE__)+'/../environment.rb'
class Importer
  def import_headed_tsvs(location)
    return "Not a valid path: #{location}" if !File.exists?(location)
    mysql_filename = "mysql_tmp_#{Time.now.to_i}_#{rand(10000)}.sql"
    mysql_file = File.open(mysql_filename, "w+")
    if File.file?(location) && location.include?(".tsv")
      import_headed_tsv(location, mysql_file)
    else
      #a folder
      Sh::bt("ls #{location}").split("\n").each do |file|
        import_headed_tsv(location+"/"+file, mysql_file) if file.include?(".tsv")
      end
    end
    mysql_file.close
    start = Time.now
    puts "Executing mysql block..."
    config = DataMapper.repository.adapter.options
    Sh::sh("mysql -u #{config["user"]} --password='#{config["password"]}' -P #{config["port"]} -h #{config["host"]} #{config["path"].gsub("/", "")} < #{mysql_filename}")
    puts "Executed mysql block (#{Time.now-start} seconds)."
  end
  
    def import_headed_tsv(location, mysql_file)
    header = CSV.open(location, :col_sep => "\t", :row_sep => "\0", :quote_char => '"').first
    model = map_to_model(header)
    mysql_file.write("load data infile '#{location}' into table #{model.storage_name} fields terminated by '\\t' optionally enclosed by '\"' lines terminated by '\\0' ignore 1 lines (#{header.join(", ")});\n")
  end
  
  def map_to_model(fields)
    tables = {}
    DataMapper.repository.adapter.select("show tables").each do |table|
      tables[table] = DataMapper.repository.adapter.select("show fields from #{table}").collect{|f| f.field}
    end
    result = []
    tables.values.collect{|t| result=t if result.length < (t&fields).length}
    return tables.invert[result] && tables.invert[result].classify.constantize || nil
  end
  
  def convert_to_tsvs(location)
    return "Not a valid path: #{location}" if !File.exists?(location)
    if File.file?(location)
      convert_to_tsv(location)
    else
      #a folder
      Sh::bt("ls #{location}").split("\n").each do |file|
        puts "#{Sh::bt("ls #{location}").split("\n").index(file)}/#{Sh::bt("ls #{location}").split("\n").length}..."
        convert_to_tsv(location+"/"+file)
      end
    end
  end
  
  def convert_to_tsv(location)
    file = File.open(location.gsub(".csv", ".tsv"), "w+")
    tsv = CSV.new(file, :col_sep => "\t", :row_sep => "\0", :quote_char => '"')
    CSV.open(location).each do |row|
      tsv << row
    end    
    file.close
  end
end
gg = Importer.new
puts "Converting Tweet..."
gg.import_headed_tsvs("/Users/dgaffney/raw/tweet")
puts "Converting User..."
gg.import_headed_tsvs("/Users/dgaffney/raw/user")
puts "Converting Geo..."
gg.import_headed_tsvs("/Users/dgaffney/raw/geo")
puts "Converting Coordinate..."
gg.import_headed_tsvs("/Users/dgaffney/raw/coordinate")
puts "Converting Entity..."
gg.import_headed_tsvs("/Users/dgaffney/raw/entity")