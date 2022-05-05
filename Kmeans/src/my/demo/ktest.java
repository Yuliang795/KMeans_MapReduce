package my.demo;

import java.io.BufferedReader;
//import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.ArrayList;
//import java.util.StringTokenizer;

//import java.io.FileReader;
import java.util.concurrent.ThreadLocalRandom;
//import java.util.stream.Stream;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ktest {
    public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private  ArrayList<Point> center_list = new ArrayList<>();
        // define nearest index and point as Text variable
        private Text point_ = new Text();
        private Text index_ = new Text();

        // In the setup section, read the center points file and convert the string
        // center file lines to Point class and store them in an array list.
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            // retrive file path
            Path centroids = new Path(conf.get("centroid.path"));
            // create a filesystem object
            FileSystem fs = FileSystem.get(conf);
            // create a file reader
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);
            // read centroids from the file and store them in a centroids variable
            Text key = new Text();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                center_list.add(new Point(key.toString()));
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input param: key is the position in the data file, value is the line at the position
            // convert the input line from str to Point class. Then iterate the center points to compare
            // the point with each center points and store the neaest center point. Pass the result to
            // Reducer in the format (key, value) /(index_, point_)

            // convert the input line from str to Point class
            Point current_point = new Point(value.toString());
            // Iterate the center points, compare the input point with each of them and store
            // the nearest center point
            Double tmp_distance = 0.0d;
            int nearest_center_index = -1;
            Double min_distance = Double.POSITIVE_INFINITY;
            for (int index =0; index<center_list.size(); index++){
                tmp_distance = current_point.get_distance_to(center_list.get(index));
                if (tmp_distance<min_distance){
                    min_distance = tmp_distance;
                    nearest_center_index = index;
                }
            }
            // output the center index as the key and the point as the value
            index_.set(String.valueOf(nearest_center_index));
            point_.set(current_point.toString());
            // pass the result to Reducer
            context.write(index_,point_); //(key and value) -> (index_, point_)
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }

    }

    public static class PointsReducer extends Reducer<Text, Text, Text, Text> {
        // create custom counters to monitor the process
        public static enum Counter {
            CONVERGED,
            TOTAL_LAUNCHED_MAPS,
            TOTAL_LAUNCHED_REDUCES

        }
        // create variable to store the new centroids
        private  ArrayList<String> new_center_list = new ArrayList<>();

        // create final new center key&value to return
        private final Text centerKey = new Text();
        private final Text centerValue = new Text();

        // This function takes string data point and convert it to double
        public Double[] get_double(Text point_in){
            String[] point_in_str = point_in.toString().split(",");
            Double[] point = {Double.parseDouble(point_in_str[0]),Double.parseDouble(point_in_str[1])};
            return point;
        }

        @Override
        public void setup(Context context) {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Input: key -> centroid id/centroid , value -> list of points
            // calculate the new centroid
            // new_centroids.add() (store updated cetroid in a variable)

            // Input parameter: key is the cluster id, value is the set of points belong to the cluster
            // For each cluster, iterate all points within it to calculate the average point and return
            // the average point as the new center of the cluster.
            Double sum_x = 0.0d;
            Double sum_y = 0.0d;
            Double[] point = new Double[5]; // dimension of each line, for 2-d data should be 2
            // increase the counter defined in previous section by 1
            context.getCounter(Counter.CONVERGED).increment(1);
            // define counter to count the number of points within the cluster, this value is then used
            // to calculate the average point.
            int counter = 0;
            // for each group, sum up the points and find the new avg point
            while(values.iterator().hasNext()){
                Text tmp_value = values.iterator().next();
                counter++;
                //sum over each dimension of the data.
                // Get Double x value and y value by get_double. This function returns a double[]
                //  where the first argument is x value and the second is the y value.
                point = get_double(tmp_value);
                sum_x+=point[0];
                sum_y+=point[1];
            }
            // find the avg of x and y value. And set the value to the string form xavg, yavg
            centerValue.set(String.valueOf(sum_x/counter)+","+String.valueOf(sum_y/counter));
            // the key is the cluster id
            centerKey.set(key);
            // return the key and the value to the loop
            context.write(centerKey,centerValue);
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }


    // Read centers file
    // This function takse configuration and the file path of the center points file and read the file by lines.
    // Each line of the string point is converted to Point class. As a result, it returns an array of center Points.
    public static ArrayList<Point> read_centers(String filePath, Configuration conf) throws IOException, InstantiationException, IllegalAccessException {
        // arraylist to store the points
        ArrayList<Point> center_list = new ArrayList<>();
        // read target file
        Path path = new Path(filePath);
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sq_reader = new SequenceFile.Reader(fs, path, conf);
        WritableComparable key = (WritableComparable) sq_reader.getKeyClass().newInstance();
        Writable value = (Writable) sq_reader.getValueClass().newInstance();
        // iterate the centers , convert the centers to point class, and store the Points to a list
        while (sq_reader.next(key, value)){
            center_list.add(new Point(key.toString()));
        }
        return center_list;
    }

    // This function takes the configuration and the target file path(center points result) as input.
    // This function read the target file by lines, and store each line as Point class. The list of
    // center points result will be returned.
    // The target file is in the format of key value pair (using \t to separate them)
    public static ArrayList<Point> read_result(Configuration conf, String filePath) throws IOException{
        // arraylist to store the points
        ArrayList<Point> center_list = new ArrayList<>();
        // read target file
        Path path = new Path(filePath);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null){
            String[] tmp = line.split("\t"); // target file uses \t format
            center_list.add(new Point(tmp[1]));   // only need the value part (x,y coordinate)
            line = br.readLine();
        }
        return center_list;
    }

    // Compare two lists of Point objects to determine if they meet stopping criteria (converges)
    // In this demo, the algorithm converges if the Euclidean distance between each new center point
    // and the old center point is less than 1% of the euclidean distance of the old center point
    // list1 is the previous center list | list2 is the latest(current) center list
    public static boolean point_lsit_compare(ArrayList<Point> list1, ArrayList<Point> list2){
        Boolean comp_flag = true;
        for (int i=0; i<list1.size();i++){
            Double dist = list1.get(i).get_change_percent(list2.get(i));
            comp_flag = (dist<=0.01);
            // display the comparison result of each center point
//            System.out.println("--->compare distance -->    --bollean: (distance:"+dist+" <1%) : "+comp_flag);
            // if one center point fail the test, (one false obtained), break the loop and return false.
            if(! comp_flag){
                break;
            }
        }
        return comp_flag;
    }

    // write new centers into seq file
    // This function takes the configuration, target new centers storage path, and a list of new center points.
    // The fucntion create a sequence file at the target path and write the center points to the file.
    // Note: list1 is the list of the latest/current center points
    public static void write_to_seq(Configuration conf, String filePath, ArrayList<Point> list1) throws IOException {
        Path center_path = new Path(filePath);
        FileSystem fs = FileSystem.get(conf);
        final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class, IntWritable.class);
        final IntWritable value = new IntWritable(0);
        // iterate over the center points list and write the points to sequence file
        for (Point center : list1){
            centerWriter.append(new Text(center.toString()), value);
        }
        centerWriter.close();
    }

    // create initial centers
    // In this demo, the initial centers are randomly selected from the dataset. Note: This method is very inefficient
    // and has bad performance to the result. Other initial strategy should be considered.
    // This function takes the path of the dataset as the input_file_path, and take the number of centers as Knum.
    // The function read through the target data file and generate 5 random index of the data file to get correspoing
    // random initial centers.
    public static ArrayList<Point> initial_centers(Configuration conf, String input_file_path, int Knum) throws IOException {
        Path file_path = new Path(input_file_path);
        FileSystem fs = file_path.getFileSystem(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file_path)));

        // get 5 random numbers within the range of #total lines
        // (1) get the number of total lines of the file
        ArrayList<String> line_collect = new ArrayList<String>();
        String line = br.readLine();
        int line_counter = 0;
        while (line != null){
            line_counter++;
            line_collect.add(line);
            line = br.readLine();
        }
        // (2) generate list of centers by random index
        ArrayList<Point> center_list = new ArrayList<Point>();
        String random_point =null;
        for (int i =0;i<5;i++) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, line_counter + 1);
        // get the value of random centers by index
            random_point=line_collect.get(randomNum);
            center_list.add(new Point(random_point));
        }
        return center_list; // return the list of random centers
    }

    // Main part of the algorithm
    // Initiate the job and execute the job in a loop until stopping criteria reached.

    public static void main(String[] args) throws Exception {
        // set configuration
        Configuration conf = new Configuration();
        // Define the path to store temporary center sequence as centroid/cen.seq
        // This file is used to compare the previous centers with the new centers
        Path center_path = new Path("centroid/cen.seq");
        conf.set("centroid.path", center_path.toString());
        // set the file system variable
        FileSystem fs = FileSystem.get(conf);

        // ========= set up path by command line input arguments
        // argument#1: input data path
        // argument#2: output file path
        // path of input data
        String initial_input_path = args[0]+"/data_points.txt";
        // path of output file
        String output_path=args[1]+"/part-r-00000";
        // Path of output directory, set this to args[1]
        Path path_out = new Path(args[1]);

        // can not override in hadoop. if output path/file exist -> del the path
        // check if the output file already exists
        if (fs.exists(path_out)){
            fs.delete(path_out, true);
        }
        // check if the temporary center path/file already exists and delete the path if true
        if (fs.exists(center_path)) {
            fs.delete(center_path, true);
        }

        // set up initial points
        final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class, IntWritable.class);
        final IntWritable value = new IntWritable(0);
        // get a list of random centers from the file by calling initial_centers()
        ArrayList<Point> initial_centers_list = initial_centers(conf,initial_input_path, 5);
        // add selected centers to the seq file
        for (Point center:initial_centers_list){
            centerWriter.append(new Text(center.toString()), value);
//            System.out.println("added"+center.toString());
        }
        centerWriter.close();
        // define converge flag| if set to true, kmeans converges, else not converges
        boolean conv_flag=false;
        // itr max number of iteration, when reach itr, the loop will stop
        int itr =0;
        // start the while loop
        while ( itr <20){
            // delete the output path if exist
            if (fs.exists(path_out)){
                fs.delete(path_out, true);
            }
            // initiate maper and reducer
            Job job = Job.getInstance(conf, "kmean");
            job.setJarByClass(ktest.class);
            job.setMapperClass(PointsMapper.class);
            job.setReducerClass(PointsReducer.class);
            // file path
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // get the job status, true-> success, false-> fail
            boolean result = job.waitForCompletion(true);

            // if the job is failed, close the application
            // else if the job succeeded, proceed to next step
            if(!result) {
                System.out.println("application failed");
                System.exit(1);
            }
            //  read the seq file to get previous centers   ---> *previous centers points
            ArrayList<Point> t1 = read_centers("centroid/cen.seq", conf);
            // read the output result file to get current centers    ---> current centers/latest result
            // Note: output_path is the path for ouput file i.e. "output/part-r-00000"
            ArrayList<Point> t2 = read_result(conf, output_path);
            // compare two list of centers  // arg1 is the previous center list  | arg2 is the current center list/latest result
            conv_flag = point_lsit_compare(t1,t2);
            System.out.println("iter: "+itr+ " successive center distance < 1%: "+conv_flag);

            // if two center lists are not same, write the new center list into the file and del the old one
            // else, break the loop and the current center list is the final result
            if (!conv_flag){
                // Not converge -> del center output path
                // store the current center list to the temporary sequence file for comparison in next iteration
                if (fs.exists(center_path)) {
                    fs.delete(center_path, true);
                }
                write_to_seq(conf, "centroid/cen.seq",t2);
            }else{
                break;
            }
            itr++;
        }
        // *** print result centers report
        // read the centroid file from hdfs and print the centroids (final result)
        ArrayList<Point> final_result = read_result(conf, output_path);
        String final_center_report = "======Final Result======\nTotal iteration: "+itr+"\nconverged: "+conv_flag+"\nFinal centers:\n";
        for (Point p:final_result){
            final_center_report+=p.toString()+"\n";
        }
        // display the result
        System.out.println(final_center_report);
        // close application
        System.exit(0);
    }
}

