/*****************************************************************************************
 * @file  Query.java
 *
 */

 import static java.lang.System.out;

 /*****************************************************************************************
  * The Query class loads the University Database.
  */
 class UniversityDBQuery
 {
     /*************************************************************************************
      * Main method for loading a previously saved Movie Database.
      * @param args  the command-line arguments
      */
     public static void main (String [] args)
     {
         out.println ();
 
         //load tables saved in UniversityDB
         var student = Table.load ("student");
         var course = Table.load ("course");
         var takes = Table.load ("takes");
 
         
         student.print();
         course.print();
         takes.print();


        // 6 iterations (throw away iteration 1 (due to JIT), and average the rest
        long startTime = 0;
        long endTime;
        long [] time = {0, 0, 0, 0, 0};
        long totalTime = 0;

        for (int i = 0; i < 6; i++) {
            if (i > 0) {
                startTime = System.nanoTime();
            }
            Table student_select = student;
            if (Table.mType.equals(Table.MapType.NO_MAP)) {
                student_select = student.select ("status == status840791"); //this condition should be change to be a strong, weak, and medium filter on student (& double check that there is at least one student for which P is true or the query just returns nothing!)
            } else {
                student_select = student.select(new KeyType(student.tuples.get(1)[0])); //test wtih primary key (change index within get)
            }
            
            student_select.print();

            out.println ();
            var st_join = new Table(null, null, null, null, null);
            out.println(Table.mType);
            int foreignKey = 0;
            int primaryKey = 0;
            switch (Table.mType) {
                case Table.MapType.NO_MAP: st_join = student_select.join(takes);
                    break;
                case Table.MapType.LINHASH_MAP:
                    for (int j = 0; j < takes.attribute.length; j++) {
                        if (student_select.col(takes.attribute[j]) != -1) {
                            foreignKey = student_select.col(takes.attribute[j]);
                            primaryKey = j;
                        }
                    } //for
                    st_join = student_select.i_join(student_select.attribute[foreignKey], takes.attribute[primaryKey] , takes);
                    break;
                default: 
                    for (int j = 0; j < takes.attribute.length; j++) {
                        if (student_select.col(takes.attribute[j]) != -1) {
                            foreignKey = student_select.col(takes.attribute[j]);
                            primaryKey = j;
                        }
                    } //for
                    st_join = student_select.i_join(student_select.attribute[foreignKey], takes.attribute[primaryKey] , takes);
                    break;
            }
            
            st_join.print ();

            out.println ();
            foreignKey = 0;
            primaryKey = 0;
            var stc_join = new Table(null, null, null, null, null);
            switch (Table.mType) {
                case Table.MapType.NO_MAP: stc_join = st_join.join(course);
                    break;
                case Table.MapType.LINHASH_MAP:
                    for (int j = 0; j < course.attribute.length; j++) {
                        if (st_join.col(course.attribute[j]) != -1) {
                            foreignKey = st_join.col(course.attribute[j]);
                            primaryKey = j;
                        }
                    } //for
                    stc_join = st_join.i_join(st_join.attribute[foreignKey], course.attribute[primaryKey] , takes);
                    break;
                default:
                    for (int j = 0; j < course.attribute.length; j++) {
                        if (st_join.col(course.attribute[j]) != -1) {
                            foreignKey = st_join.col(course.attribute[j]);
                            primaryKey = j;
                        }
                    } //for
                    stc_join = st_join.i_join(st_join.attribute[foreignKey], course.attribute[primaryKey] , takes);
                    break;
            }
           
            stc_join.print ();

            out.println ();
            var stc_project = stc_join.project ("cname");
            stc_project.print ();

            if (i > 0) {
                endTime = System.nanoTime();
                time[i - 1] = (endTime - startTime);
                totalTime += (endTime - startTime);
            }
        } //for
        long avg = totalTime / 5; //average of 5 iterations
        System.out.println("-------------------------------\nMap Type = " + Table.mType + "\n-------------------------------"); //Run with different mappings to see which is faster
        for (int l = 0; l < 5; l++) {
            System.out.println("Run " + l + ": " + time[l] + " ns");
        } //for
        System.out.println("-------------------------------\nAverage Time: " + avg + " ns");
        System.out.println("Number of Tuples: " + (student.tuples.size() + course.tuples.size() + takes.tuples.size())); //change tuple generated amount in UniversityDB
    
     } // main
 
 } // UniversityDBQuery
 
 
