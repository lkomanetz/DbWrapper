using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DbWrapper;
using System.Data;
using DbWrapper.Read;
using System.Xml;
using DbWrapper.Serializer;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
			//Database db = new Database("208.107.242.5",
			//						   "videostore",
			//						   DatabaseEngine.MySQL,
			//						   "lkomanetz",
			//						   "K0manetz3");

			//Record.SearchProperty = "Title";
			//Record rec = new Record("inventory", db);
			//rec.AddQuery("Title/=/Test");
			//rec.AddJoin("Users",
			//			"ID",
			//			"CreatedBy",
			//			JoinType.Inner);
			//rec.Read();

			//if (rec.Next()) {
			//	Console.WriteLine(rec.Get<string>("Title"));
			//}
			Database db = new Database("208.107.242.5",
										"FusionDB",
										DatabaseEngine.SqlServer,
										"Fusion_SA",
										"Passw0rd");
			Record rec = new Record("Requests", db);
			rec.AddQuery("ID/=/2", "Users");
			rec.AddJoin("Users",
						"ID",
						"CreatedBy",
						JoinType.Inner);
			rec.Read();
            Console.ReadLine();
        //    TimeSpan elapsedTime = new TimeSpan();

        //    for (short i = 0; i < 50; i++)
        //    {
        //        Stopwatch entireTime = new Stopwatch();
        //        entireTime.Start();
        //        Stopwatch watch = new Stopwatch();
        //        watch.Start();
        //        Database db = new Database("208.107.242.5",
        //                                    "MobileTimePunch",
        //                                    "SQL Server",
        //                                    "MobileTimePunch_SA",
        //                                    "Passw0rd");
        //        Record.SearchProperty = "ID";

        //        try
        //        {
        //            Record rec = new Record("schedule", db);
        //            rec.AddQuery("user_id/=/" + 1);
        //            rec.Read();

        //            while (rec.Next())
        //            {
        //                TimeSpan span = rec.Get<TimeSpan>("schedule_time");
        //                Console.WriteLine(String.Format("Retrieved Value: {0}", span));
        //            }
        //            watch.Stop();
        //            Console.WriteLine("Read Time: " + watch.Elapsed);
        //            //Console.ReadLine();
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        watch.Reset();
        //        watch.Start();

        //        Record createRec = new Record("user", db);
        //        createRec.Set<string>("first_name", "First Name");
        //        createRec.Set<string>("last_name", "Last Name");
        //        createRec.Set<string>("username", "fname");
        //        createRec.Set<string>("password", "pass");
        //        createRec.Create();

        //        watch.Stop();
        //        Console.WriteLine("Create Time: " + watch.Elapsed);
        //        //Console.ReadLine();
        //        watch.Reset();

        //        watch.Start();
        //        Record updateRec = new Record("user", db);
        //        updateRec.AddQuery("first_name/=/First Name", ClauseType.And);
        //        updateRec.AddQuery("last_name/=/Last Name");
        //        updateRec.Read();

        //        if (updateRec.Next())
        //        {
        //            updateRec.Set<string>("first_name", "First Name 1");
        //            updateRec.Update();
        //        }
        //        watch.Stop();
        //        Console.WriteLine("Update Time: " + watch.Elapsed);
        //        //Console.ReadLine();
        //        watch.Reset();

        //        watch.Start();
        //        Record deleteRec = new Record("user", db);

        //        deleteRec.AddQuery("first_name/=/First Name 1");
        //        deleteRec.Read();

        //        if (deleteRec.Next())
        //            deleteRec.Remove();
        //        watch.Stop();
        //        Console.WriteLine("Delete Time: " + watch.Elapsed);
        //        //Console.ReadLine();

        //        entireTime.Stop();
        //        elapsedTime += entireTime.Elapsed;
        //    }
        //    Console.WriteLine("\n50 iteration time: " + elapsedTime);
        //    Console.ReadLine();
        }
    }
}
