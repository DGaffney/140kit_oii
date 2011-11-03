load File.dirname(__FILE__)+'/../environment.rb'
class Filter < Instance

  MAX_TRACK_IDS = 10000
  BATCH_SIZE = 100
  STREAM_API_URL = "http://stream.twitter.com"
  CHECK_FOR_NEW_DATASETS_INTERVAL = 60*10
  
  attr_accessor :user_account, :username, :password, :next_dataset_ends, :queue, :params, :datasets, :start_time, :last_start_time

  def initialize
    super
    @datasets = []
    @queue = []
    oauth_settings = YAML.load(File.read(File.dirname(__FILE__)+'/../config/twitter.yml'))
    account = oauth_settings.keys.shuffle.first
    TweetStream.configure do |config|
      config.consumer_key = oauth_settings[account]["oauth_settings"]["consumer_key"]
      config.consumer_secret = oauth_settings[account]["oauth_settings"]["consumer_secret"]
      config.oauth_token = oauth_settings[account]["access_token"]["access_token"]
      config.oauth_token_secret = oauth_settings[account]["access_token"]["access_token_secret"]
      config.auth_method = :oauth
      config.parser   = :yajl
    end
    at_exit { do_at_exit }
  end
  
  def do_at_exit
    puts "Exiting."
    save_queue
    @user_account.unlock
    @datasets.collect{|dataset| dataset.unlock}
  end
  
  def filt
    puts "Filtering..."
    check_in
    assign_user_account
    puts "Entering filter routine."
    loop do
      if !killed?
        stream_routine
      else
        puts "Just nappin'."
        sleep(SLEEP_CONSTANT)
      end
    end
  end
  
  def stream_routine
    add_datasets
    clean_up_datasets
    if !@datasets.empty?
      update_next_dataset_ends
      update_params
      collect
      save_queue
      clean_up_datasets
    end
  end
  
  def assign_user_account
    puts "Assigning user account."
    message = true
    while @screen_name.nil?
      user = AuthUser.unlocked.first
      if !user.nil? && user.lock
        @user_account = user
        @screen_name = user.screen_name
        @password = user.password
        puts "Assigned #{@screen_name}."
      else
        answer = Sh::clean_gets_yes_no("No twitter accounts available. Add one now?") if message
        if answer
          first_attempt = true
          while answer!="y" || first_attempt
            first_attempt = false
            puts "Enter your screen name:"
            screen_name = Sh::clean_gets
            puts "Enter your password:"
            password = Sh::clean_gets
            puts "We got the username '#{screen_name}' and a password that was #{password.length} characters long. Sound right-ish? (y/n)"
            answer = Sh::clean_gets
          end
          puts "Creating new AuthUser..."
          user = AuthUser.new(:screen_name => screen_name, :password => password)
          user.save
          user = AuthUser.unlocked.first
          @user_account = user
          @screen_name = user.screen_name
          @password = user.password
          puts "Assigned #{@screen_name}."
        else
          puts "Then I can't do anything for you. May god have mercy on your data"
          exit!
        end
        message = false
      end
      sleep(5)
    end
  end
  
  def collect
    @start_time = Time.now
    puts "Collecting: #{params_for_stream.inspect}"
    client = TweetStream::Client.new
    client.on_interval(CHECK_FOR_NEW_DATASETS_INTERVAL) { rsync_previous_files; @start_time = Time.now; puts "Switching to new files..."; client.stop if add_datasets }
    client.on_limit { |skip_count| puts "\nWe are being rate limited! We lost #{skip_count} tweets!\n" }
    client.on_error { |message| puts "\nError: #{message}\n";client.stop }
    client.filter(params_for_stream) do |tweet|
#      puts "[tweet] #{tweet[:user][:screen_name]}: #{tweet[:text]}"
      @queue << tweet
      save_queue if @queue.length >= BATCH_SIZE
      if @next_dataset_ends
        client.stop if U.times_up?(@next_dataset_ends)
      end
    end
    save_queue
  end
  
  def params_for_stream
    params = {}
    @params.each {|k,v| params[k.to_sym] = v.collect {|x| x[:params] } }
    return params
  end
  
  def save_queue
    if !@queue.empty?
      puts "Saving #{@queue.length} tweets."
      tweets, users, entities, geos, coordinates = data_from_queue
      @queue = []
      Thread.new {
        dir = lambda{|model| File.dirname(__FILE__)+'/../../../data/raw/'+model+"/"+@username+"_"+@start_time.strftime("%Y-%m-%d_%H-%M-%S")}
        Tweet.store_to_flat_file(tweets, dir.call("tweet"))
        User.store_to_flat_file(users, dir.call("user"))
        Entity.store_to_flat_file(entities, dir.call("entity"))
        Geo.store_to_flat_file(geos, dir.call("geo"))
        Coordinate.store_to_flat_file(coordinates, dir.call("coordinate"))
      }
    end
  end
  
  def rsync_previous_files
    rsync_job = fork do
      dir = lambda{|model| File.dirname(__FILE__)+'/../../../data/raw/'+model+"/"+@username+"_"+@start_time.strftime("%Y-%m-%d_%H-%M-%S")}
      [Tweet, User, Entity, Geo, Coordinate].each do |model|
        `rsync #{dir.call(model.to_s.downcase)}.csv gonkclub@nutmegunit.com:oii/raw_data/#{model.to_s.downcase}/#{@username+"_"+@start_time.strftime("%Y-%m-%d_%H-%M-%S")}.csv`
#        `rm #{dir.call(model.to_s.downcase)}.csv`
      end
    end
    Process.detach(rsync_job)
  end

  def data_from_queue
    tweets = []
    users = []
    entities = []
    geos = []
    coordinates = []
    @queue.each do |json|
      tweet, user = TweetHelper.prepped_tweet_and_user(json)
      geo = GeoHelper.prepped_geo(json)
      dataset_id = {:dataset_id => determine_dataset(json)}
      tweets << tweet.merge(dataset_id)
      users << user.merge(dataset_id)
      geos << geo.merge(dataset_id)
      coordinates = coordinates+CoordinateHelper.prepped_coordinates(json).collect{|coordinate| coordinate.merge(dataset_id)}
      entities = entities+EntityHelper.prepped_entities(json).collect{|entity| entity.merge(dataset_id)}
    end
    tweets.uniq! {|t| t[:twitter_id] }
    users.uniq! {|u| u[:twitter_id] }
    return tweets, users, entities, geos, coordinates
  end
  
  def update_params
    @params = {}
    for d in @datasets
      if @params[d.scrape_type]
        if d.scrape_type == "locations"
          @params[d.scrape_type] << {:params => d.params.split(",")[0..d.params.split(",").length-2].join(","), :dataset_id => d.id}
        else
          @params[d.scrape_type] << {:params => d.params.split(",").first, :dataset_id => d.id}
        end
      else
        if d.scrape_type == "locations"
          @params[d.scrape_type] = [{:params => d.params.split(",")[0..d.params.split(",").length-2].join(","), :dataset_id => d.id}]
        else
          @params[d.scrape_type] = [{:params => d.params.split(",").first, :dataset_id => d.id}]
        end
      end
    end
  end
  
  def determine_dataset(tweet)
    return @datasets.first.id if @datasets.length == 1
    if @params.has_key?("locations")
      if tweet[:place]
        for location in @params["locations"]
          if in_location?(location[:params], tweet[:place][:bounding_box][:coordinates].first)
            return location[:dataset_id]
          end
        end
      end
    end
    if @params.has_key?("track")
      for term in @params["track"]
        if tweet[:text].include?(term[:params])
          return term[:dataset_id]
        end
      end
    end
    if @params.has_key?("follow")
      for user_id in @params["follow"]
        if tweet[:user][:id] == user_id[:params].to_i
          return user_id[:dataset_id]
        end
      end
    end
    return nil
  end
  
  def in_location?(location_params, tweet_location)
    search_location = location_params.split(",").map {|c| c.to_i }
    t_longs = tweet_location.collect {|a| a[0] }.uniq.sort
    t_lats = tweet_location.collect {|a| a[0] }.uniq.sort
    t_long_range = (t_longs.first..t_longs.last)
    t_lat_range = (t_lats.first..t_lats.last)
    l_long_range = (search_location[0]..search_location[2])
    l_lat_range = (search_location[1]..search_location[3])
    return (l_long_range.include?(t_long_range) && l_lat_range.include?(t_lat_range))
  end
  
  # def in_bounding_box?(location_params)
  #   t = self[:place][:bounding_box][:coordinates].first
  #   s = location_params.split(",").map {|c| c.to_f }
  #   a = { :left => t[0][0],
  #         :bottom => t[0][1],
  #         :right => t[2][0],
  #         :top => t[2][1] }
  #   b = { :left => s[0],
  #         :bottom => s[1],
  #         :right => s[2],
  #         :top => s[3] }
  #   abxdif = ((a[:left]+a[:right])-(b[:left]+b[:right])).abs
  #   abydif = ((a[:top]+a[:bottom])-(b[:top]+b[:bottom])).abs
  #   xdif = (a[:right]+b[:right])-(a[:left]+b[:left])
  #   ydif = (a[:top]+b[:top])-(a[:bottom]+b[:bottom])
  #   return (abxdif <= xdif && abydif <= ydif)
  # end
  
  def add_datasets
    datasets = Dataset.unlocked.all(:scrape_finished => false, :scrape_type => ['track', 'follow', 'locations'])
    return claim_new_datasets(datasets)
  end

  def claim_new_datasets(datasets)
    # distribute datasets evenly
    return false if datasets.empty?
    num_instances = Instance.count(:instance_type => "streamer", :killed => false)
    datasets_per_instance = num_instances.zero? ? datasets.length : (datasets.length.to_f / num_instances.to_f).ceil
    datasets_to_claim = datasets[0..datasets_per_instance]
    if !datasets_to_claim.empty?
     claimed_datasets = Dataset.lock(datasets_to_claim)
     if !claimed_datasets.empty?
       update_datasets(claimed_datasets)
       return true
     end
    end
    return false
  end
   
  def update_datasets(datasets)
    @datasets = @datasets|datasets
    if @datasets.length > MAX_TRACK_IDS
      denied_datasets = []
      @datasets -= (denied_datasets = @datasets[MAX_TRACK_IDS-1..datasets.length])
      unlock(denied_datasets)
    end
  end

  def update_next_dataset_ends
    update_start_times
    refresh_datasets # this is absolutely necessary even while it's called in update_start_times above. huh!
    soonest_ending_dataset = @datasets.select{|d| d.params.split(",").last.to_i!=-1}.sort {|x,y| (x.created_at.to_time.gmt + x.params.split(",").last.to_i - DateTime.now.to_time.gmt) <=> (y.created_at.to_time.gmt + y.params.split(",").last.to_i - DateTime.now.to_time.gmt) }.first
    @next_dataset_ends = soonest_ending_dataset.created_at.to_time.gmt + soonest_ending_dataset.params.split(",").last.to_i rescue nil
  end

  def update_start_times
    refresh_datasets
    datasets_to_be_started = @datasets.select {|d| d.created_at.nil? }
    # Dataset.update_all({:created_at => DateTime.now.in_time_zone}, {:id => datasets_to_be_started.collect {|d| d.id}})
    Dataset.all(:id => datasets_to_be_started.collect {|d| d.id}).update(:created_at => Time.now)
    refresh_datasets
  end

  def refresh_datasets
    @datasets = Dataset.all(:id => @datasets.collect {|d| d.id })
  end

  def clean_up_datasets
    started_datasets = @datasets.reject {|d| d.created_at.nil? }
    finished_datasets = started_datasets.select{|d| d.params.split(",").last.to_i!=-1}.select {|d| U.times_up?(d.created_at.gmt+d.params.split(",").last.to_i) }
    if !finished_datasets.empty?
      puts "Finished collecting "+finished_datasets.collect {|d| "#{d.scrape_type}:\"#{d.internal_params_label}\"" }.join(", ")
      # Dataset.update_all({:scrape_finished => true}, {:id => finished_datasets.collect {|d| d.id}})
      Dataset.all(:id => finished_datasets.collect {|d| d.id}).update(:scrape_finished => true)
      @datasets -= finished_datasets
      finished_datasets.collect{|dataset| dataset.unlock}
    end
  end
  
end

filter = Filter.new
filter.username = "dgaff"
filter.filt
