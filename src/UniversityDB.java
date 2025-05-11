import static java.lang.System.out;
import java.util.ArrayList;
import java.util.List;
public class UniversityDB {
       /*************************************************************************************
     * The main method is the driver for TestGenerator.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
        var project = new TupleGeneratorImpl ();

        project.addRelSchema ("Student",
                           "sid sname address status",
                           "Integer String String String",
                           "id",
                           null);
        
        
        project.addRelSchema ("Course",
                           "cid cname deptId descr",
                           "Integer String String String",
                           "cid",
                           null);
        
        project.addRelSchema ("Takes",
                           "sid cid",
                           "Integer Integer",
                           "sid cid",
                           new String [][] {{ "sid", "Student", "sid"},
                                            { "cid", "Course", "cid" }});
    

        var tables = new String [] { "Student", "Course", "Takes" };
        var tups   = new int [] { 20000, 40000, 60000}; //CHANGE These Numbers to increase tuple generation size! (Try to keep {student < takes < course} input values)
        
        var tuples = project.generate (tups);
       
        List <Comparable []> studentRows = new ArrayList<>();
        List <Comparable []> courseRows = new ArrayList<>();
        List <Comparable []> takesRows = new ArrayList<>();;  
        
        Table student = new Table ("Student", "sid sname address status", "Integer String String String", "sid");
        for (var tuple : tuples[0]) {
            student.insert(tuple);
        }
        student.print();
        student.save();

        Table course = new Table ("Course", "cid cname deptId descr", "Integer String String String", "cid");
        for (var tuple : tuples[1]) {
            course.insert(tuple);
        }
        course.print();
        course.save();
        
        Table takes = new Table ("Takes", "sid cid", "Integer Integer", "sid cid");
        for (var tuple : tuples[2]) {
            takes.insert(tuple);
        }
        takes.print();
        takes.save();

        out.println ();

    } // main
}
