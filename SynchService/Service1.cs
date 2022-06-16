using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.IO;
using System.Net;
using System.Data.SqlClient;
using System.Security.Cryptography;
using System.Timers;
using System.Threading;

namespace SynchService
{
    public partial class Service1 : ServiceBase
    {
        System.Timers.Timer timer_version; // name space(using System.Timers;)
        System.Timers.Timer timer_patch;
        System.Timers.Timer timer_queue;

        Thread versionThread = null;
        Thread patchThread = null;
        Thread queueThread = null;

        static readonly string ServiceLog = "ServiceLog";
        static string log_query = "";
        static string shore_log_query = "";
        static string ftphost = "ftp://50.28.66.162";//"ftp://20.219.3.205";
        static string VesselName = "Vessel1";
        static int VesselID = 0;
        static NetworkCredential Credentials = new NetworkCredential("OrionERPShore", "orion");
        //static readonly NetworkCredential Credentials = new NetworkCredential("ERPShoreDevelopment", "orionshoreDev");
        static readonly string hostqueue = "Queue/";
        static string localPath = "";// @"D:\Office\misslenious\VPS_SHIP\Queue";
        static string databasecredential = "";
        static int Count_Ver = 0;
        static int Count_Pat = 0;
        static int Count_Upload = 0;
        static int Count_Download = 0;
        static int Connection_Count_Ver = 0;
        static int Connection_Count_Pat = 0;
        static int Connection_Count_Upload = 0;
        static int Connection_Count_Download = 0;
        static List<string> directories_ver = new List<string>();
        static List<string> directories_pat = new List<string>();
        static List<string> directories_download = new List<string>();
        static List<string> directories_upload = new List<string>();
        public Service1()
        {
            InitializeComponent();

            // Get NAU Root Path
            localPath = AppDomain.CurrentDomain.BaseDirectory; //Application.StartupPath;

            //Read Config File for Connection String
            string server = "";
            string database = "";
            string UID = "";
            string password = "";
            WriteToFile(localPath + " found" + DateTime.Now, ServiceLog);
            try
            {
                if (File.Exists(localPath + "/Web.config"))
                {
                    WriteToFile("Web config found" + DateTime.Now, ServiceLog);
                    string sst = File.ReadAllText(localPath + "/Web.config");

                    foreach (string line in File.ReadAllLines(localPath + "/Web.config"))
                    {
                        if (line.Contains("<add key=\"sqlconnectionstring\"") && !line.Contains("<!--"))
                        {
                            int val = line.IndexOf("Data Source");
                            string str1 = line.Substring(val, line.Length - val - 3);

                            string[] str_list = str1.Split(';');
                            server = str_list[0].Split('=')[1];
                            database = str_list[1].Split('=')[1];
                            UID = str_list[2].Split('=')[1];
                            password = str_list[3].Split('=')[1];

                            break;
                        }

                    }
                    WriteToFile("Establishing database connection " + DateTime.Now, ServiceLog);
                    databasecredential = "server=" + server + ";database=" + database + ";UID=" + UID + ";password=" + password + "";

                    String query = "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE  TABLE_NAME = 'tbl_config'))BEGIN select vesselID,vesselName,(select value from tbl_config where name='SyncFTP') as SyncFTP,(select value from tbl_config where name='SyncFTPUser') as SyncFTPUser,(select value from tbl_config where name='SyncFTPPassword') as SyncFTPPassword,'1' as 'IS_FTP' from vessel End Else Begin select vesselID,vesselName,'0' as 'IS_FTP' from vessel END";

                    SqlConnection con = new SqlConnection(databasecredential);
                    con.Open();
                    SqlDataAdapter da = new SqlDataAdapter(query, con);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    da.Dispose();
                    con.Close();
                    WriteToFile("Connection established " + DateTime.Now, ServiceLog);
                    VesselName = ds.Tables[0].Rows[0]["VesselName"].ToString();
                    VesselID = (Int32)ds.Tables[0].Rows[0]["VesselID"];
                    if (ds.Tables[0].Rows[0]["IS_FTP"].ToString() == "1")
                    {
                        if (ds.Tables[0].Rows[0]["SyncFTP"].ToString() != "" && ds.Tables[0].Rows[0]["SyncFTPUser"].ToString() != "" && ds.Tables[0].Rows[0]["SyncFTPPassword"].ToString() != "")
                        {
                            ftphost = ds.Tables[0].Rows[0]["SyncFTP"].ToString();
                            Credentials = new NetworkCredential(ds.Tables[0].Rows[0]["SyncFTPUser"].ToString(), ds.Tables[0].Rows[0]["SyncFTPPassword"].ToString());
                        }
                    }

                    WriteToFile(VesselName + " found " + "- FTP Details -" + ftphost + "  " + DateTime.Now, ServiceLog);

                    string sql_log_table = "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE  TABLE_NAME = 'tbl_synchronizer_log'))BEGIN Create Table tbl_synchronizer_log(ID int identity(1, 1),VesselID int,Log_type nvarchar(200),Log_Status nvarchar(100),Log_Msg nvarchar(max),CreatedOn datetime,timestamp datetime) END";
                    ExecuteScalar(sql_log_table);
                    WriteToFile("Log table checked and created " + DateTime.Now, ServiceLog);
                }

            }
            catch (Exception ex)
            {

                WriteToFile("Exception Occured " + ex.Message + " " + DateTime.Now, ServiceLog);
            }

        }

        protected override void OnStart(string[] args)
        {
            AddToOutGoingQueueForQuery_Ver(VesselID, "Service", "Started", "-", "tbl_synchronizer_log", "ID");

            // setup for version update
            timer_version = new System.Timers.Timer();
            timer_version.Elapsed += new ElapsedEventHandler(OnElapsedTime_Version);
            timer_version.Interval = 262800000; //73 hours number in milisecinds  
            timer_version.Enabled = true;

            // setup for patch update
            timer_patch = new System.Timers.Timer();
            timer_patch.Elapsed += new ElapsedEventHandler(OnElapsedTime_Patch);
            timer_patch.Interval = 32400000;//86400000; //9 hours number in milisecinds  
            timer_patch.Enabled = true;

            //setup for Queue update
            timer_queue = new System.Timers.Timer();
            timer_queue.Elapsed += new ElapsedEventHandler(OnElapsedTime_Queue);
            timer_queue.Interval = 14400000; //4 hours number in milisecinds  
            timer_queue.Enabled = true;

        }
        protected override void OnStop()
        {
            AddToOutGoingQueueForQuery_Ver(VesselID, "Service", "Stopped", "-", "tbl_synchronizer_log", "ID");
            WriteToFile("Service is stopped at " + DateTime.Now, ServiceLog);

            //versionThread.Abort();
            //timer_version.Stop();

            //patchThread.Abort();
            //timer_patch.Stop();

            //queueThread.Abort();
            //timer_queue.Stop();
        }

        #region Version Download
        private void OnElapsedTime_Version(object source, ElapsedEventArgs e)
        {
            AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", "Version Download function Called", "tbl_synchronizer_log", "ID");
            Connection_Count_Ver = 0;
            versionThread = new Thread(new ThreadStart(DownloadVersions));
            versionThread.Start();
        }
        public static void DownloadVersions()
        {
            FtpWebResponse response = null;
            StreamReader streamReader = null;
            string VersionPath = ftphost + "/Version/";
            try
            {
                //Version Download

                Count_Ver = 0;
                // Adding file name to the dictionary from shore vessel

                FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(VersionPath);
                ftpRequest.Credentials = Credentials;
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;

                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", "Shore Path - " + VersionPath, "tbl_synchronizer_log", "ID");

                response = (FtpWebResponse)ftpRequest.GetResponse();
                streamReader = new StreamReader(response.GetResponseStream());

                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", "Connection Established", "tbl_synchronizer_log", "ID");

                directories_ver = new List<string>();

                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line))
                {
                    string[] lineArr = line.Split('/');
                    line = lineArr[lineArr.Length - 1];
                    directories_ver.Add(line);
                    line = streamReader.ReadLine();
                    Count_Ver++;
                }
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", Count_Ver + " Files found in verion shore", "tbl_synchronizer_log", "ID");
                streamReader.Close();
                response.Close();
                Download_Ver(VersionPath, localPath, Credentials);

            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Error", "Exception Occured " + VersionPath + " " + ex.Message, "tbl_synchronizer_log", "ID");
                //throw ex;
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Ver > 0)
                {
                    Connection_Count_Ver++;
                    if (Connection_Count_Ver < 10)
                    {
                        DownloadVersions();
                    }
                }
                streamReader.Close();
                response.Close();
            }
        }
        public static void Download_Ver(string ParentFolderpath, string localPath, NetworkCredential Credentials)
        {
            try
            {
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", "Checking Vessel Version files to compare", "tbl_synchronizer_log", "ID");
                // Getting Version List from Onboard
                IEnumerable<string> localFiles = Directory.GetFiles(localPath + "/Version").Select(path => Path.GetFileName(path));
                IEnumerable<string> localFiles_archieve = Directory.GetFiles(localPath + "/Version/Archieve").Select(path => Path.GetFileName(path));
                IEnumerable<string> localFiles_Final = localFiles.Union(localFiles_archieve);
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", localFiles_Final.Count() + " Files are found in Version", "tbl_synchronizer_log", "ID");
                // Comparing files
                IEnumerable<string> missingFiles = directories_ver.Except(localFiles_Final);

                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", missingFiles.Count() + " Files to be donloaded from shore after comparision", "tbl_synchronizer_log", "ID");
                //Downloading missing files from Shore Vessel
                foreach (string filename in missingFiles)
                {
                    //Console.WriteLine("Downloading missing file {0}", filename);
                    AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", filename + " Start Downloading", "tbl_synchronizer_log", "ID");
                    string remoteFileUri = ParentFolderpath + "/" + filename;
                    string IncomingFilePath = Path.Combine(localPath + "/Version", filename);

                    FtpWebRequest DownloadRequest = (FtpWebRequest)WebRequest.Create(remoteFileUri);
                    DownloadRequest.Credentials = Credentials;
                    DownloadRequest.Method = WebRequestMethods.Ftp.DownloadFile;
                    using (FtpWebResponse downloadResponse = (FtpWebResponse)DownloadRequest.GetResponse())
                    using (Stream sourceStream = downloadResponse.GetResponseStream())
                    using (Stream targetStream = File.Create(IncomingFilePath))
                    {
                        byte[] buffer = new byte[10240];
                        int read;
                        while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            targetStream.Write(buffer, 0, read);
                        }
                    }
                    AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Started", filename + " Downloaded to Vessel Version", "tbl_synchronizer_log", "ID");
                    Count_Ver--;
                }
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Success", "Download Completed", "tbl_synchronizer_log", "ID");
            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_Ver(VesselID, "Version", "Error", "Exception in downloading to Vessel - " + ParentFolderpath + " " + ex.Message, "tbl_synchronizer_log", "ID");
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Ver > 0)
                {
                    Connection_Count_Ver++;
                    if (Connection_Count_Ver < 10)
                    {
                        Download_Ver(ParentFolderpath, localPath, Credentials);
                    }
                }
            }
        }
        public static bool AddToOutGoingQueueForQuery_Ver(int VesselID, string Log_Type, string Log_Status, string Log_msg, string tablename, string fieldname)
        {
            bool b = false;

            try
            {
                string today_datetime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
                log_query = "insert into tbl_synchronizer_log(VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) Output Inserted.ID values(" + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string retval = ExecuteScalar(log_query);
                shore_log_query = "insert into tbl_synchronizer_log(ID,VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) values(" + retval + "," + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_log_query) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                ExecuteNonQuery(sql);

                if (!string.IsNullOrEmpty(tablename))
                {
                    sql = "update " + tablename + " set timestamp=getdate() where " + fieldname + "=" + retval;
                    ExecuteNonQuery(sql);
                }

                // Log old records deletion
                int count_log = Convert.ToInt32(ExecuteScalar("select COUNT(ID) from tbl_synchronizer_log"));
                if (count_log > 1000)
                {
                    string shore_sql_delete_old_log = "delete from tbl_synchronizer_log where VesselID=" + VesselID + " and ID not in ( select top 1000 ID from tbl_synchronizer_log where VesselID=" + VesselID + " order by ID desc)";
                    string shore_sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_sql_delete_old_log) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                    ExecuteNonQuery(shore_sql);

                    string sql_delete_old_log = "delete from tbl_synchronizer_log where ID not in ( select top 1000 ID from tbl_synchronizer_log order by ID desc)";
                    ExecuteNonQuery(sql_delete_old_log);
                }
            }
            catch (Exception ex)
            {
                WriteToFile("Add to out going query got exception - " + ex.Message, ServiceLog);
                return b;
            }

            return b;
        }
        #endregion

        #region Patch Download
        private void OnElapsedTime_Patch(object source, ElapsedEventArgs e)
        {
            AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", "Patch Download function Called", "tbl_synchronizer_log", "ID");
            Connection_Count_Pat = 0;
            patchThread = new Thread(new ThreadStart(DownloadPatch));
            patchThread.Start();
        }
        public static void DownloadPatch()
        {
            FtpWebResponse response = null;
            StreamReader streamReader = null;
            string PatchPath = ftphost + "/patches/" + VesselName;
            try
            {
                //Version Download

                Count_Pat = 0;
                // Adding file name to the dictionary from shore vessel
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", "Entered in Patch download function", "tbl_synchronizer_log", "ID");

                FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(PatchPath);
                ftpRequest.Credentials = Credentials;
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;

                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", "Shore Path - " + PatchPath, "tbl_synchronizer_log", "ID");

                response = (FtpWebResponse)ftpRequest.GetResponse();
                streamReader = new StreamReader(response.GetResponseStream());

                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", "Connection Established", "tbl_synchronizer_log", "ID");

                directories_pat = new List<string>();

                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line))
                {
                    string[] lineArr = line.Split('/');
                    line = lineArr[lineArr.Length - 1];
                    directories_pat.Add(line);
                    line = streamReader.ReadLine();
                    Count_Pat++;
                }

                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", Count_Pat + " Patch Files found in verion shore", "tbl_synchronizer_log", "ID");

                streamReader.Close();
                response.Close();
                Download_Pat(PatchPath, localPath, Credentials);

            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Error", "Exception Occured " + PatchPath + " " + ex.Message, "tbl_synchronizer_log", "ID");

                //throw ex;
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Pat > 0)
                {
                    Connection_Count_Pat++;
                    if (Connection_Count_Pat < 10)
                    {
                        DownloadPatch();
                    }

                }
                if (!ex.Message.Contains("file not found"))
                {
                    AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Error", "No patch file found ", "tbl_synchronizer_log", "ID");
                }
                streamReader.Close();
                response.Close();
            }
        }
        public static void Download_Pat(string ParentFolderpath, string localPath, NetworkCredential Credentials)
        {
            try
            {
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", "Checking Vessel Patch files to compare", "tbl_synchronizer_log", "ID");
                // Getting Version List from Onboard
                IEnumerable<string> localFiles = Directory.GetFiles(localPath + "/Version").Select(path => Path.GetFileName(path));
                IEnumerable<string> localFiles_archieve = Directory.GetFiles(localPath + "/Version/Archieve").Select(path => Path.GetFileName(path));
                IEnumerable<string> localFiles_Final = localFiles.Union(localFiles_archieve);
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", localFiles_Final.Count() + " Files are found in Patch", "tbl_synchronizer_log", "ID");

                // Comparing files
                IEnumerable<string> missingFiles = directories_pat.Except(localFiles_Final);

                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", missingFiles.Count() + " Files to be donloaded from shore after comparision", "tbl_synchronizer_log", "ID");

                //Downloading missing files from Shore Vessel
                foreach (string filename_Patch in missingFiles)
                {
                    //Console.WriteLine("Downloading missing file {0}", filename);
                    AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", filename_Patch + " Start Downloading", "tbl_synchronizer_log", "ID");

                    string remoteFileUri = ParentFolderpath + "/" + filename_Patch;
                    string IncomingFilePath = Path.Combine(localPath + "/Version", filename_Patch);

                    FtpWebRequest DownloadRequest = (FtpWebRequest)WebRequest.Create(remoteFileUri);
                    DownloadRequest.Credentials = Credentials;
                    DownloadRequest.Method = WebRequestMethods.Ftp.DownloadFile;
                    using (FtpWebResponse downloadResponse = (FtpWebResponse)DownloadRequest.GetResponse())
                    using (Stream sourceStream = downloadResponse.GetResponseStream())
                    using (Stream targetStream = File.Create(IncomingFilePath))
                    {
                        byte[] buffer = new byte[10240];
                        int read;
                        while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            targetStream.Write(buffer, 0, read);
                        }
                    }
                    AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Started", filename_Patch + " Downloaded to Vessel Patch", "tbl_synchronizer_log", "ID");
                    Count_Pat--;
                }
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Success", "Download Completed", "tbl_synchronizer_log", "ID");
            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_Pat(VesselID, "Patch", "Error", "Exception in downloading to Vessel- " + ParentFolderpath + " " + ex.Message, "tbl_synchronizer_log", "ID");
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Pat > 0)
                {
                    Connection_Count_Pat++;
                    if (Connection_Count_Pat < 10)
                    {
                        Download_Pat(ParentFolderpath, localPath, Credentials);
                    }
                }
            }
        }
        public static bool AddToOutGoingQueueForQuery_Pat(int VesselID, string Log_Type, string Log_Status, string Log_msg, string tablename, string fieldname)
        {
            bool b = false;


            try
            {
                string today_datetime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
                log_query = "insert into tbl_synchronizer_log(VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) Output Inserted.ID values(" + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string retval = ExecuteScalar(log_query);
                shore_log_query = "insert into tbl_synchronizer_log(ID,VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) values(" + retval + "," + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_log_query) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                ExecuteNonQuery(sql);

                if (!string.IsNullOrEmpty(tablename))
                {
                    sql = "update " + tablename + " set timestamp=getdate() where " + fieldname + "=" + retval;
                    ExecuteNonQuery(sql);
                }
                // Log old records deletion
                int count_log = Convert.ToInt32(ExecuteScalar("select COUNT(ID) from tbl_synchronizer_log"));
                if (count_log > 1000)
                {
                    string shore_sql_delete_old_log = "delete from tbl_synchronizer_log where VesselID=" + VesselID + " and ID not in ( select top 1000 ID from tbl_synchronizer_log where VesselID=" + VesselID + " order by ID desc)";
                    string shore_sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_sql_delete_old_log) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                    ExecuteNonQuery(shore_sql);

                    string sql_delete_old_log = "delete from tbl_synchronizer_log where ID not in ( select top 1000 ID from tbl_synchronizer_log order by ID desc)";
                    ExecuteNonQuery(sql_delete_old_log);
                }
            }
            catch (Exception ex)
            {
                WriteToFile("Add to out going query got exception - " + ex.Message, ServiceLog);
                return b;
            }

            return b;
        }
        #endregion

        #region Queue Download
        private void OnElapsedTime_Queue(object source, ElapsedEventArgs e)
        {
            AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue", "Started", "Queue function Called", "tbl_synchronizer_log", "ID");
            Connection_Count_Download = 0;
            queueThread = new Thread(new ThreadStart(DownloadQueueFiles));
            queueThread.Start();

            Connection_Count_Upload = 0;
            queueThread = new Thread(new ThreadStart(UploadQueueFiles));
            queueThread.Start();
        }
        public static void DownloadQueueFiles()
        {
            FtpWebResponse response = null;
            StreamReader streamReader = null;
            string ParentFolderpath = ftphost + "/" + hostqueue + "/" + VesselName + "/Outgoing_archive";
            try
            {
                Count_Download = 0;
                // Adding file name to the dictionary from shore vessel

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", "Queue Download function Called", "tbl_synchronizer_log", "ID");

                FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(ParentFolderpath);
                ftpRequest.Proxy = null;
                ftpRequest.Credentials = Credentials;
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", "Shore Path - " + ParentFolderpath, "tbl_synchronizer_log", "ID");

                response = (FtpWebResponse)ftpRequest.GetResponse();
                streamReader = new StreamReader(response.GetResponseStream());

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", "Connection Established", "tbl_synchronizer_log", "ID");

                directories_download = new List<string>();

                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line))
                {
                    string[] lineArr = line.Split('/');
                    line = lineArr[lineArr.Length - 1];
                    directories_download.Add(line);
                    line = streamReader.ReadLine();
                    Count_Download++;
                }

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", Count_Download + " Files found in outgoing archive shore", "tbl_synchronizer_log", "ID");

                streamReader.Close();
                response.Close();
                Download(ParentFolderpath, localPath, Credentials);

            }
            catch (Exception ex)
            {

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Error", "Exception Occured " + ParentFolderpath + " " + ex.Message, "tbl_synchronizer_log", "ID");

                //throw ex;
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Download > 0)
                {
                    Connection_Count_Download++;
                    if (Connection_Count_Download < 10)
                    {
                        DownloadQueueFiles();
                    }
                }
                streamReader.Close();
                response.Close();
            }
        }
        public static void Download(string ParentFolderpath, string localPath, NetworkCredential Credentials)
        {
            try
            {
                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", "Checking Vessel Incoming archieve files to compare", "tbl_synchronizer_log", "ID");

                // Getting Incoming Archieve List from Onboard
                IEnumerable<string> localFiles = Directory.GetFiles(localPath + "/Queue/Incoming_archive").Select(path => Path.GetFileName(path));
                // Getting Incoming List from Onboard
                IEnumerable<string> localFiles_incoming = Directory.GetFiles(localPath + "/Queue/Incoming").Select(path => Path.GetFileName(path));

                IEnumerable<string> localFiles_Final = localFiles.Union(localFiles_incoming);

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", localFiles_Final.Count() + " Files are found in Incoming & Incoming Archieve", "tbl_synchronizer_log", "ID");
                // Comparing files
                IEnumerable<string> missingFiles = directories_download.Except(localFiles_Final);

                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", missingFiles.Count() + " Files to be donloaded from shore after comparision", "tbl_synchronizer_log", "ID");

                //Downloading missing files from Shore Vessel
                foreach (string filename_Download in missingFiles)
                {
                    //Console.WriteLine("Downloading missing file {0}", filename);
                    AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", filename_Download + " Start Downloading", "tbl_synchronizer_log", "ID");

                    string remoteFileUri = ParentFolderpath + "/" + filename_Download;
                    string IncomingFilePath = Path.Combine(localPath + "/Queue/Incoming", filename_Download);

                    FtpWebRequest DownloadRequest = (FtpWebRequest)WebRequest.Create(remoteFileUri);
                    DownloadRequest.Proxy = null;
                    DownloadRequest.Credentials = Credentials;
                    DownloadRequest.Method = WebRequestMethods.Ftp.DownloadFile;
                    using (FtpWebResponse downloadResponse = (FtpWebResponse)DownloadRequest.GetResponse())
                    using (Stream sourceStream = downloadResponse.GetResponseStream())
                    using (Stream targetStream = File.Create(IncomingFilePath))
                    {
                        byte[] buffer = new byte[10240];
                        int read;
                        while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            targetStream.Write(buffer, 0, read);
                        }
                    }

                    AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Started", filename_Download + " Downloaded to Vessel incoming", "tbl_synchronizer_log", "ID");
                    Count_Download--;
                }
                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Success", "Download Completed", "tbl_synchronizer_log", "ID");
            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_Queue_Download(VesselID, "Queue Download", "Error", "Exception in downloading to Vessel- " + ParentFolderpath + " " + ex.Message, "tbl_synchronizer_log", "ID");

                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Download > 0)
                {
                    Connection_Count_Download++;
                    if (Connection_Count_Download < 10)
                    {
                        Download(ParentFolderpath, localPath, Credentials);
                    }
                }
            }
        }
        public static bool AddToOutGoingQueueForQuery_Queue_Download(int VesselID, string Log_Type, string Log_Status, string Log_msg, string tablename, string fieldname)
        {
            bool b = false;

            try
            {
                string today_datetime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
                log_query = "insert into tbl_synchronizer_log(VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) Output Inserted.ID values(" + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";

                string retval = ExecuteScalar(log_query);
                shore_log_query = "insert into tbl_synchronizer_log(ID,VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) values(" + retval + "," + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_log_query) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                ExecuteNonQuery(sql);

                if (!string.IsNullOrEmpty(tablename))
                {
                    sql = "update " + tablename + " set timestamp=getdate() where " + fieldname + "=" + retval;
                    ExecuteNonQuery(sql);
                }
                // Log old records deletion
                int count_log = Convert.ToInt32(ExecuteScalar("select COUNT(ID) from tbl_synchronizer_log"));
                if (count_log > 1000)
                {
                    string shore_sql_delete_old_log = "delete from tbl_synchronizer_log where VesselID=" + VesselID + " and ID not in ( select top 1000 ID from tbl_synchronizer_log where VesselID=" + VesselID + " order by ID desc)";
                    string shore_sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_sql_delete_old_log) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                    ExecuteNonQuery(shore_sql);

                    string sql_delete_old_log = "delete from tbl_synchronizer_log where ID not in ( select top 1000 ID from tbl_synchronizer_log order by ID desc)";
                    ExecuteNonQuery(sql_delete_old_log);
                }
            }
            catch (Exception ex)
            {
                WriteToFile("Add to out going query got exception - " + ex.Message, ServiceLog);
                return b;
            }

            return b;
        }
        #endregion

        #region Queue Upload
        public static void UploadQueueFiles()
        {
            FtpWebResponse response = null;
            StreamReader streamReader = null;
            FtpWebResponse response_incoming = null;
            StreamReader streamReader_incoming = null;

            string ParentFolderpath1 = ftphost + "/" + hostqueue + "/" + VesselName + "/Incoming";
            string Path_txt = ftphost + "/" + hostqueue + "/" + VesselName;

            try
            {
                Count_Upload = 0;
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", "Entered in queue file upload function", "tbl_synchronizer_log", "ID");

                // Getting Incoming Archieve List
                string ParentFolderpath = ftphost + "/" + hostqueue + "/" + VesselName + "/Incoming_archive";
                FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(ParentFolderpath);
                ftpRequest.Proxy = null;
                ftpRequest.Credentials = Credentials;
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;

                response = (FtpWebResponse)ftpRequest.GetResponse();
                streamReader = new StreamReader(response.GetResponseStream());

                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", "Connection Established for Incoming Archieve", "tbl_synchronizer_log", "ID");

                directories_upload = new List<string>();
                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line))
                {
                    string[] lineArr = line.Split('/');
                    line = lineArr[lineArr.Length - 1];
                    directories_upload.Add(line);
                    line = streamReader.ReadLine();
                    Count_Upload++;
                }

                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", Count_Upload + " Files found in incoming archieve shore", "tbl_synchronizer_log", "ID");

                streamReader.Close();
                response.Close();

                //Getting incoming file list 
                FtpWebRequest ftpRequest_incoming = (FtpWebRequest)WebRequest.Create(ParentFolderpath1);
                ftpRequest_incoming.Proxy = null;
                ftpRequest_incoming.Credentials = Credentials;
                ftpRequest_incoming.Method = WebRequestMethods.Ftp.ListDirectory;
                response_incoming = (FtpWebResponse)ftpRequest_incoming.GetResponse();
                streamReader_incoming = new StreamReader(response_incoming.GetResponseStream());

                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", "Connection Established for Incoming Shore", "tbl_synchronizer_log", "ID");

                string line_incoming = streamReader_incoming.ReadLine();
                while (!string.IsNullOrEmpty(line_incoming))
                {
                    string[] lineArr = line_incoming.Split('/');
                    line_incoming = lineArr[lineArr.Length - 1];
                    directories_upload.Add(line_incoming);
                    line_incoming = streamReader_incoming.ReadLine();
                    Count_Upload++;
                }

                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", "Total " + Count_Upload + " Files found in incoming  & Incoming archieve shore", "tbl_synchronizer_log", "ID");
                response_incoming.Close();
                streamReader_incoming.Close();

                Upload(ParentFolderpath1, localPath, Credentials, Path_txt);
            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Error", "Exception Occured in Upload " + ParentFolderpath1 + " " + ex.Message, "tbl_synchronizer_log", "ID");

                //throw ex;
                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Upload > 0)
                {
                    Connection_Count_Upload++;
                    if (Connection_Count_Upload < 10)
                    {
                        UploadQueueFiles();
                    }
                }
                streamReader.Close();
                response.Close();

                response_incoming.Close();
                streamReader_incoming.Close();
            }
        }
        public static void Upload(string ParentFolderpath, string localPath, NetworkCredential Credentials, string Path_txt)
        {
            try
            {
                //Comparing files
                IEnumerable<string> localFiles = Directory.GetFiles(localPath + "/Queue/Outgoing_archive").Select(path => Path.GetFileName(path));
                IEnumerable<string> missingFiles = localFiles.Except(directories_upload);

                // Uploading missing files to shore vessel Folder
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Started", missingFiles.Count() + " Files to be upload on shore incoming", "tbl_synchronizer_log", "ID");

                foreach (string filename_Upload in missingFiles)
                {
                    //Console.WriteLine("Uploading missing file {0}", filename);
                    AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue", "Started", filename_Upload + " Start Uploading", "tbl_synchronizer_log", "ID");

                    string IncomingFilePath = ParentFolderpath + "/" + filename_Upload;
                    string localFilePath = Path.Combine(localPath + "/Queue/Outgoing_archive", filename_Upload);

                    FtpWebRequest uploadRequest = (FtpWebRequest)WebRequest.Create(IncomingFilePath);
                    uploadRequest.Proxy = null;
                    uploadRequest.Credentials = Credentials;
                    uploadRequest.Method = WebRequestMethods.Ftp.UploadFile;
                    using (Stream targetStream = uploadRequest.GetRequestStream())
                    using (Stream sourceStream = File.OpenRead(localFilePath))
                    {
                        byte[] buffer = new byte[10240];
                        int read;
                        while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            targetStream.Write(buffer, 0, read);
                        }
                    }
                    Count_Upload--;
                    AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue", "Started", filename_Upload + " Uploaded to shore incoming", "tbl_synchronizer_log", "ID");
                }
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Queue Upload", "Success", "Queue Upload Completed", "tbl_synchronizer_log", "ID");


                // Upload log file to shore vessel folder
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Logfile", "Started", "Uploading Log File to Shore Vessel Folder", "tbl_synchronizer_log", "ID");


                IEnumerable<string> logfile = Directory.GetFiles(localPath + "/SynchronizerLog").Select(path => Path.GetFileName(path));
                if (logfile.Count() > 0)
                {
                    foreach (string filename_log in logfile)
                    {
                        AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Logfile", "Success", filename_log + " Found!", "tbl_synchronizer_log", "ID");
                        FtpWebRequest uploadRequest_txt = (FtpWebRequest)WebRequest.Create(Path_txt + "/" + filename_log);
                        uploadRequest_txt.Proxy = null;
                        uploadRequest_txt.Credentials = Credentials;
                        uploadRequest_txt.Method = WebRequestMethods.Ftp.UploadFile;
                        string localtxtPath = Path.Combine(localPath + "/SynchronizerLog", filename_log);
                        using (Stream targetStream = uploadRequest_txt.GetRequestStream())
                        using (Stream sourceStream = File.OpenRead(localtxtPath))
                        {
                            byte[] buffer = new byte[10240];
                            int read;
                            while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                targetStream.Write(buffer, 0, read);
                            }
                        }
                        AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Logfile", "Success", filename_log + " uploaded to Shore Vessel Folder", "tbl_synchronizer_log", "ID");
                    }

                }

            }
            catch (Exception ex)
            {
                AddToOutGoingQueueForQuery_QueueUpload(VesselID, "Logfile", "Error", "Exception in uploading to shore- " + Path_txt + " " + ex.Message, "tbl_synchronizer_log", "ID");

                if (ex.Message.Contains("Unable to connect to the remote server") || Count_Upload > 0)
                {
                    Connection_Count_Upload++;
                    if (Connection_Count_Upload < 10)
                    {
                        Upload(ParentFolderpath, localPath, Credentials, Path_txt);
                    }

                }
            }
        }
        public static bool AddToOutGoingQueueForQuery_QueueUpload(int VesselID, string Log_Type, string Log_Status, string Log_msg, string tablename, string fieldname)
        {
            bool b = false;


            try
            {
                string today_datetime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
                log_query = "insert into tbl_synchronizer_log(VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) Output Inserted.ID values(" + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";


                string retval = ExecuteScalar(log_query);
                shore_log_query = "insert into tbl_synchronizer_log(ID,VesselID,Log_type,Log_Status,Log_Msg,CreatedOn) values(" + retval + "," + VesselID + ",'" + Log_Type + "','" + Log_Status + "','" + Log_msg + "','" + today_datetime + "')";
                string sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_log_query) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                ExecuteNonQuery(sql);

                if (!string.IsNullOrEmpty(tablename))
                {
                    sql = "update " + tablename + " set timestamp=getdate() where " + fieldname + "=" + retval;
                    ExecuteNonQuery(sql);
                }
                // Log old records deletion
                int count_log = Convert.ToInt32(ExecuteScalar("select COUNT(ID) from tbl_synchronizer_log"));
                if (count_log > 1000)
                {
                    string shore_sql_delete_old_log = "delete from tbl_synchronizer_log where VesselID=" + VesselID + " and ID not in ( select top 1000 ID from tbl_synchronizer_log where VesselID=" + VesselID + " order by ID desc)";
                    string shore_sql = "insert into tbl_outgoing(data,tablename,tableid,autofield)values('" + Encrypt(shore_sql_delete_old_log) + "','" + tablename + "'," + retval + ",'" + fieldname + "')";
                    ExecuteNonQuery(shore_sql);

                    string sql_delete_old_log = "delete from tbl_synchronizer_log where ID not in ( select top 1000 ID from tbl_synchronizer_log order by ID desc)";
                    ExecuteNonQuery(sql_delete_old_log);
                }
            }
            catch (Exception ex)
            {
                WriteToFile("Add to out going query got exception - " + ex.Message, ServiceLog);
                return b;
            }

            return b;
        }
        #endregion


        public static void WriteToFile(string Message, string filename)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\SynchronizerLog";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\SynchronizerLog\\" + filename + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        public static string ExecuteScalar(string sql)
        {
            SqlConnection cnn = new SqlConnection(databasecredential);
            SqlCommand cmd = new SqlCommand(sql, cnn);
            cmd.CommandTimeout = 500000;
            cnn.Open();
            string retval = Convert.ToString(cmd.ExecuteScalar());
            cmd.Dispose();
            cnn.Close();
            return retval;
        }
        public static int ExecuteNonQuery(string sql)
        {
            SqlConnection cnn = new SqlConnection(databasecredential);
            SqlCommand cmd = new SqlCommand(sql, cnn);
            cmd.CommandTimeout = 500000;
            cnn.Open();
            int retval = cmd.ExecuteNonQuery();
            cmd.Dispose();
            cnn.Close();
            return retval;
        }

        public static string Encrypt(string plainText)
        {

            string passPhrase = "A1B2C3D4@";
            string saltValue = "@D1C3B2A1";
            string initVector = "@B12CD34EF56GH78";
            string hashAlgorithm = "SHA1";
            int keySize = 256;
            int passwordIterations = 2;



            byte[] initVectorBytes = null;
            initVectorBytes = Encoding.ASCII.GetBytes(initVector);

            byte[] saltValueBytes = null;
            saltValueBytes = Encoding.ASCII.GetBytes(saltValue);

            // Convert our plaintext into a byte array.
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);

            PasswordDeriveBytes password = new PasswordDeriveBytes(passPhrase, saltValueBytes, hashAlgorithm, passwordIterations);

            // Use the password to generate pseudo-random bytes for the encryption key.
            byte[] keyBytes = password.GetBytes(keySize / 8);

            // Create uninitialized Rijndael encryption object.
            RijndaelManaged symmetricKey = default(RijndaelManaged);
            symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform encryptor = default(ICryptoTransform);
            encryptor = symmetricKey.CreateEncryptor(keyBytes, initVectorBytes);

            MemoryStream memoryStream = default(MemoryStream);
            memoryStream = new MemoryStream();

            CryptoStream cryptoStream = default(CryptoStream);
            cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write);
            // Start encrypting.
            cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);

            // Finish encrypting.
            cryptoStream.FlushFinalBlock();

            // Convert our encrypted data from a memory stream into a byte array.
            byte[] cipherTextBytes = memoryStream.ToArray();

            // Close both streams.
            memoryStream.Close();
            cryptoStream.Close();

            // Convert encrypted data into a base64-encoded string.
            string cipherText = System.Convert.ToBase64String(cipherTextBytes);

            // Return encrypted string.
            return cipherText;
        }
    }
}
