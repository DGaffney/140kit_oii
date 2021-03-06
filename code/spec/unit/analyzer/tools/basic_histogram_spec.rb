describe BasicHistogram do
  it "should set_variables" do
    analysis_metadata, analytical_offering_function, curation = initialize_analytical_test
    analytical_offering_function.set_variables(analysis_metadata, curation).class.should == Array
    clear_test
  end
  
  it "should run" do
    analysis_metadata, analytical_offering_function, curation = initialize_analytical_test
    Analysis::Dependencies.send(analysis_metadata.function)
    analytical_offering_function.set_variables(analysis_metadata, curation)
    run_vars = AnalyticalOfferingVariable.all.sort{|x,y| x.position<=>y.position}.collect{|v| v.value}
    analytical_offering_function.run(*run_vars).class.should == Hash
    clear_test
  end
  
  def initialize_analytical_test
    analytical_offering_function = BasicHistogram
    analytical_offering = AnalyticalOffering.first(:function => analytical_offering_function.underscore)
    [Researcher, Curation, Dataset, User, Tweet, Graph, GraphPoint, Edge, AnalysisMetadata, Mail, AnalyticalOfferingVariable].collect{|model|
      model.destroy
    }
    researcher = Researcher.gen
    curation = Curation.gen
    dataset = Dataset.gen
    curation.datasets << dataset
    analysis_metadata = AnalysisMetadata.new
    analysis_metadata.curation_id = curation.id
    analysis_metadata.analytical_offering_id = analytical_offering.id
    analysis_metadata.save!
    users = []
    tweets = []
    1.upto(100) do |user|
      user = User.gen
      user_tweets = []
      1.upto(rand(3)+1) do |tweet|
        tweet = Tweet.gen
        tweet.screen_name = user.screen_name
        tweet.user_id = user.twitter_id
        tweet.dataset_id = dataset.id
        tweet.save!
        tweets << tweet
      end
      user.dataset_id = dataset.id
      user.save!
      users << user
    end
    return analysis_metadata, analytical_offering_function, curation
  end
  
  def clear_test
    [Researcher, Curation, Dataset, User, Tweet, Graph, GraphPoint, Edge, AnalysisMetadata, Mail, AnalyticalOfferingVariable].collect{|model|
      model.destroy
    }
  end
end
# 
# class BasicHistogram < AnalysisMetadata
# 
#   DEFAULT_CHUNK_SIZE = 1000
#   
#   def self.set_variables(analysis_metadata, curation)
#     remaining_variables = []
#     analysis_metadata.analytical_offering.variables.each do |variable|
#       analytical_offering_variable = AnalyticalOfferingVariable.new
#       analytical_offering_variable.analytical_offering_variable_descriptor_id = variable.id
#       analytical_offering_variable.analysis_metadata_id = analysis_metadata.id
#       case variable.name
#       when "curation_id"
#         analytical_offering_variable.value = curation.id
#         analytical_offering_variable.save
#       when "save_path"
#         analytical_offering_variable.value = "analytical_results/#{analysis_metadata.function}"
#         analytical_offering_variable.save
#       else
#         remaining_variables << variable
#       end
#     end
#     return remaining_variables
#   end
# 
#   
#   #Results: Frequency Charts of basic data on Tweets and Users per data set
#   def self.run(curation_id, save_path)
#     curation = Curation.first(:id => curation_id)
#     FilePathing.tmp_folder(curation, self.underscore)
#     self.generate_graphs([
#       {:model => Tweet, :attribute => :language},
#       {:model => Tweet, :attribute => :created_at},
#       {:model => Tweet, :attribute => :source},
#       {:model => Tweet, :attribute => :location},
#       {:model => User,  :attribute => :followers_count},
#       {:model => User,  :attribute => :friends_count},
#       {:model => User,  :attribute => :favourites_count},
#       {:model => User,  :attribute => :geo_enabled},
#       {:model => User,  :attribute => :statuses_count},
#       {:model => User,  :attribute => :lang},
#       {:model => User,  :attribute => :time_zone},
#       {:model => User,  :attribute => :created_at}
#     ], curation)
#     self.push_tmp_folder(curation.stored_folder_name)
#     self.finalize(curation)
#   end
# 
#   def self.generate_graphs(frequency_set, curation, analytic=self)
#     frequency_set = [frequency_set].flatten
#     curation_id = curation && curation.id || nil
#     graphs = []
#     frequency_set.each do |fs|
#       fs[:style] = fs[:style] || "histogram"
#       fs[:title] = fs[:title] || fs[:model].pluralize+"_"+fs[:attribute].to_s
#       fs[:conditional] = fs[:conditional] || {}
#       graph_attrs = Hash[fs.select{|k,v| Graph.attributes.include?(k)}]
#       graph = Graph.first_or_create({:curation_id => curation_id, :analysis_metadata_id => analytic.analysis_metadata(curation).id}.merge(graph_attrs))
#       graph.graph_points.destroy #can't call .new? as a condition for this, as it's created now.
#       graph.edges.destroy #can't call .new? as a condition for this, as it's created now.
#       conditional = Analysis.curation_conditional(curation).merge(fs[:conditional])
#       graphs << graph
#       if block_given?
#         yield fs, graph, conditional
#       else
#         self.frequency_graphs(fs, graph, conditional)
#       end
#     end
#     return graphs
#   end
# 
#   def self.frequency_graphs(fs, graph, conditional, path=ENV['TMP_PATH'])
#     limit = DEFAULT_CHUNK_SIZE||1000
#     offset = 0
#     sub_directory = "/"+[fs[:year],fs[:month],fs[:date],fs[:hour]].compact.join("/")
#     full_path_with_file = sub_directory == "/" ? path+"/"+graph.title+".csv" : path+sub_directory+"/"+graph.title+".csv"
#     Sh::mkdir(path+sub_directory) if sub_directory != "/"
#     FasterCSV.open(full_path_with_file, "w") do |csv|
#       records = fs[:model].aggregate(fs[:attribute], :all.count, {:limit => limit, :offset => offset}.merge(conditional))
#       graph_points = records.collect{|record| {:label => record.first, :value => record.last, :graph_id => graph.id, :curation_id => graph.curation_id}}
#       graph_points = graph.sanitize_points(graph_points)
#       while !records.empty?
#         csv << ["label", "value"]
#         graph_points.each do |graph_point|
#           csv << [graph_point[:label],graph_point[:value]]
#         end
#         GraphPoint.save_all(graph_points)
#         offset+=limit
#         records = fs[:model].aggregate(fs[:attribute], :all.count, {:limit => limit, :offset => offset}.merge(conditional))
#       end
#     end
#   end
# 
#   def self.finalize_analysis(curation)
#     response = {}
#     response[:recipient] = curation.researcher.email
#     response[:subject] = "#{curation.researcher.user_name}, the raw Graph data for the basic histograms in the \"#{curation.name}\" data set is complete."
#     response[:message_content] = "Your CSV files are ready for download. You can grab them by visiting the collection's page: <a href=\"http://140kit.com/#{curation.researcher.user_name}/collections/#{curation.id}\">http://140kit.com/#{curation.researcher.user_name}/collections/#{curation.id}</a>."
#     return response
#   end
# end
