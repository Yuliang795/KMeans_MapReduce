package my.demo;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// This is the class to store the point in coordinate form.
// This class also has functions to calculate the distance between two points and 
// calculate the percent change for stopping Criteria
public class Point {

    public double x;
    public double y;

    // constructor for empty point
    public Point(){
    }
    // constructor for string input of file lines
    //  In this demo, the input has to be a line with two points
    // assign corresponding value to x and y , and convert them to
    // Double
    public Point(String line){
        String[] points = line.split(",");
        this.x = Double.parseDouble(points[0]);
        this.y = Double.parseDouble(points[1]);
    }
    // constructor for  x and y of doulbe input
    public Point(double x, double y){
        this.x = x;
        this.y = y;
    }
    // methods to return the value of X or y
    public double getX(){
        return x;
    }
    public double getY(){
        return y;
    }
    // this function calculate the distance of this point and the
    // input point. The distance measure is euclidean distance.
    public double get_distance_to(Point target_point){
        Double x_dist = Math.pow(this.x - target_point.getX(),2);
        Double y_dist = Math.pow(this.y - target_point.getY(),2);
        Double distance = Math.sqrt(x_dist+y_dist);
        // return the result
        return distance;
    }

    // this function return the percent of change of the point and the target ponit
    // percent of change = distance of the two centers / the distance from the old
    // center to origin
    // Note: This function should be incurred by the old center point, and the
    // input point should be the new center point.
    public double get_change_percent(Point target_point){
        // distance of this point to the input point
        Double x_dist = Math.pow(this.x - target_point.getX(),2);
        Double y_dist = Math.pow(this.y - target_point.getY(),2);
        Double distance = Math.sqrt(x_dist+y_dist);

        // Euclidean distance of the current point
        Double current_dist = Math.sqrt((Math.pow(this.x,2)+Math.pow(this.y,2)));
        // find the percent of change
        Double percent_change = distance/current_dist;

        return percent_change;
    }

    @Override
    public String toString(){
        // transform the data point to string format x,y
        return String.valueOf(x)+","+String.valueOf(y);
    }
}
