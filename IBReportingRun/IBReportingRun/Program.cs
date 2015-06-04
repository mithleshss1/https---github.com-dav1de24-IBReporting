using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security;
using System.Runtime.InteropServices;
using SharpAESCrypt;
using System.Data;
using System.Data.SqlClient;
using System.Xml;
using System.Collections;
using System.Globalization;
using System.Net;
using System.Net.Mail;
using System.Configuration;

namespace IBReportingRun
{
    static class IBReportingPull
    {
        //Global eMail credentials variables
        static SecureString eMailUser = new SecureString();
        static SecureString eMailPwd = new SecureString();
        static string dbUsed = ConfigurationManager.AppSettings["UseDb"].ToString();
        static string Emails = ConfigurationManager.AppSettings["Emails"].ToString();
        //Grabs current DropBox Home local path
        static string getDropBoxHome()
        {
            var dbPath = System.IO.Path.Combine(
                            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Dropbox\\host.db");

            string[] lines = System.IO.File.ReadAllLines(dbPath);
            byte[] dbBase64Text = Convert.FromBase64String(lines[1]);
            string folderPath = System.Text.ASCIIEncoding.ASCII.GetString(dbBase64Text);

            return folderPath;
        }

        //Function to convert secure string to string
        static string ConvertToUnsecureString(this SecureString securePassword)
        {
            if (securePassword == null)
                throw new ArgumentNullException("securePassword");

            IntPtr unmanagedString = IntPtr.Zero;
            try
            {
                unmanagedString = Marshal.SecureStringToGlobalAllocUnicode(securePassword);
                return Marshal.PtrToStringUni(unmanagedString);
            }
            finally
            {
                Marshal.ZeroFreeGlobalAllocUnicode(unmanagedString);
            }
        }

        //Used to grab data from files
        static string GetLineFromFile(string filePath, int lineNumber)
        {
            string tmpLine;
            System.IO.StreamReader fileStr = new System.IO.StreamReader(filePath);

            for (int i = 1; i < lineNumber; i++) fileStr.ReadLine();
            tmpLine = fileStr.ReadLine();
            fileStr.Close();

            return tmpLine;
        }

        //Used to grab the mapping between TransactionIDs and IBIdentifiers
        static DataTable GetTranIDToIBIDMap(SqlConnection conn)
        {
            SqlCommand command = new SqlCommand("GetTransactionIdMAP", conn);
            command.CommandType = CommandType.StoredProcedure;

            conn.Open();

            SqlDataAdapter adp = new SqlDataAdapter(command);
            DataTable ds = new DataTable();
            adp.Fill(ds);

            conn.Close();

            return ds;
        }

        //Search in tranIDMapping table for a specific TransactionID
        static int GetTransactionID(DataTable appTran, string tranRaw)
        {
            //cycle through each ID to see if at least one is contained in the transaction raw string
            foreach (DataRow dr in appTran.Rows)
            {
                if (tranRaw.ToLower().Contains(dr["IBIdentifier"].ToString().ToLower())) return Convert.ToInt32(dr["ID"]);
            }

            //If we got here it means that nothing was found
            return -1;
        }

        //Print Data Table
        static void printDataTable(DataTable loadDT, string filename)
        {
            System.IO.StreamWriter sr = new System.IO.StreamWriter("C:\\Users\\Davide\\Desktop\\" + filename);

            foreach (DataRow row in loadDT.Rows)
            {
                sr.WriteLine("--- Row ---");
                foreach (var item in row.ItemArray)
                {
                    sr.Write("Item:");
                    sr.WriteLine(item); // Can I add something here to also print the column names?
                }
            }
        }

        static void UpdateTableOnDB(SqlConnection conn, string stProc, string tblName, DataTable appT)
        {
            try
            {
                SqlCommand insertCommand = new SqlCommand(
                    stProc, conn);
                insertCommand.CommandType = CommandType.StoredProcedure;

                SqlParameter tvpParam = insertCommand.Parameters.AddWithValue(
                    tblName, appT);
                tvpParam.SqlDbType = SqlDbType.Structured;

                conn.Open();

                // Execute the command.
                insertCommand.ExecuteNonQuery();

                conn.Close();
            }
            catch (Exception e)
            {
                //throw e;
                //send mail to support@appianroad.com specifying error
                SendMail("Test IBReporting Error!", "Update Process Failed for " + tblName + ". " + e.ToString(), Emails);
                Environment.Exit(0);
            }
        }

        //Cycle through XML report, parse data into tmpTables and push them to AccountMgmt DB
        static void ProcessXMLResult(SqlConnection conn, XmlDocument doc)
        {
            XmlElement root = doc.DocumentElement;
            XmlNodeList nodes = root.SelectNodes("FlexStatements");

            //Define a datatable for each of the tables that need to be updated on the AccountMgmt DB

            DataTable AccountBalanceDaily = new DataTable();
            //Create columns for accountBalanceDaily
            AccountBalanceDaily.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            AccountBalanceDaily.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            AccountBalanceDaily.Columns.Add(new DataColumn("Total", System.Type.GetType("System.Double")));
            AccountBalanceDaily.Columns.Add(new DataColumn("CashBalance", System.Type.GetType("System.Double")));

            //Grab TransactionID to IBIdentifier mapping
            DataTable TransactionIDMap = new DataTable();
            TransactionIDMap = GetTranIDToIBIDMap(conn);

            DataTable AccountTransaction = new DataTable();
            //Create columns for accountTransaction
            AccountTransaction.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            AccountTransaction.Columns.Add(new DataColumn("Balance", System.Type.GetType("System.Double")));
            AccountTransaction.Columns.Add(new DataColumn("AcctDayCount", System.Type.GetType("System.Int32")));
            AccountTransaction.Columns.Add(new DataColumn("TransactionID", System.Type.GetType("System.Int32")));
            AccountTransaction.Columns.Add(new DataColumn("TransactionRaw", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("CurrencyID", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("AssetCategoryID", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("Ticker", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("TradeID", System.Type.GetType("System.String")));
            AccountTransaction.Columns.Add(new DataColumn("Value", System.Type.GetType("System.Double")));

            DataTable TaxesMonthly = new DataTable();
            //Create columns for TaxesMonthly
            TaxesMonthly.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            TaxesMonthly.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            TaxesMonthly.Columns.Add(new DataColumn("WithholdingTax", System.Type.GetType("System.Double")));

            DataTable AccountPosition = new DataTable();
            //Create columns for AccountPosition
            AccountPosition.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            AccountPosition.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            AccountPosition.Columns.Add(new DataColumn("CurrencyID", System.Type.GetType("System.String")));
            AccountPosition.Columns.Add(new DataColumn("AssetCategoryID", System.Type.GetType("System.String")));
            AccountPosition.Columns.Add(new DataColumn("Symbol", System.Type.GetType("System.String")));
            AccountPosition.Columns.Add(new DataColumn("Description", System.Type.GetType("System.String")));
            AccountPosition.Columns.Add(new DataColumn("PositionValue", System.Type.GetType("System.Double")));
            AccountPosition.Columns.Add(new DataColumn("PositionQuantity", System.Type.GetType("System.Int32")));

            DataTable AccountTransfer = new DataTable();
            //Create columns for AccountTransfer
            AccountTransfer.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            AccountTransfer.Columns.Add(new DataColumn("CurrencyID", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("AssetCategoryID", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Symbol", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Description", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Type", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Direction", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Account", System.Type.GetType("System.String")));
            AccountTransfer.Columns.Add(new DataColumn("Quantity", System.Type.GetType("System.Double")));
            AccountTransfer.Columns.Add(new DataColumn("PositionAmount", System.Type.GetType("System.Double")));
            AccountTransfer.Columns.Add(new DataColumn("PositionAmountInBase", System.Type.GetType("System.Double")));
            AccountTransfer.Columns.Add(new DataColumn("CashTransfer", System.Type.GetType("System.Double")));
            AccountTransfer.Columns.Add(new DataColumn("AcctDayCount", System.Type.GetType("System.Int32")));


            DataTable AccountCashTransaction = new DataTable();
            //Create columns for AccountPosition
            AccountCashTransaction.Columns.Add(new DataColumn("AccountID", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("Date", System.Type.GetType("System.DateTime")));
            AccountCashTransaction.Columns.Add(new DataColumn("Type", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("Description", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("Amount", System.Type.GetType("System.Double")));
            AccountCashTransaction.Columns.Add(new DataColumn("CurrencyID", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("AssetCategoryID", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("Symbol", System.Type.GetType("System.String")));
            AccountCashTransaction.Columns.Add(new DataColumn("TradeID", System.Type.GetType("System.String")));

            var acctDateCounter = new Dictionary<string, int>();
            var acctDateCounterForTransfer = new Dictionary<string, int>();

            string curAccountId = "";

            foreach (XmlNode node in nodes)
            {
                //Cycle through each Account data
                foreach (XmlNode fxStat in node.SelectNodes("FlexStatement"))
                {

                    bool advisorAcct = false;

                    foreach (XmlElement erd in fxStat)
                    {


                        //Store variable for Advisor Account and only cycle through Client Fees items for Advisor Account
                        if (erd.Name == "AccountInformation")
                        {
                            if (erd.Attributes["accountType"].Value.ToString().ToLower().Contains("advisor master"))
                                advisorAcct = true;
                            else
                                curAccountId = erd.Attributes["accountId"].Value.ToString();
                        }

                        //Cycle through first each Account Balance Daily
                        if ((erd.Name == "EquitySummaryInBase") && (advisorAcct == false))
                        {
                            foreach (XmlNode esb in erd.SelectNodes("EquitySummaryByReportDateInBase"))
                            {
                                //Add the item to the AccountBalanceDaily data table
                                DataRow dr;
                                dr = AccountBalanceDaily.NewRow();
                                dr["AccountID"] = esb.Attributes["accountId"].Value;

                                //Format Date before inserting
                                //                    string format = "mm/dd/yyyy";

                                dr["Date"] = esb.Attributes["reportDate"].Value;
                                dr["Total"] = esb.Attributes["total"].Value;
                                dr["CashBalance"] = esb.Attributes["cash"].Value;

                                AccountBalanceDaily.Rows.Add(dr);
                            }
                        }

                        //Cycle through first each Account Position
                        if ((erd.Name == "OpenPositions") && (advisorAcct == false))
                        {
                            foreach (XmlNode esb in erd.SelectNodes("OpenPosition"))
                            {
                                DataRow dr;
                                dr = AccountPosition.NewRow();
                                dr["AccountID"] = esb.Attributes["accountId"].Value;

                                //Convert date to compatible format

                                DateTime theTime = DateTime.ParseExact(esb.Attributes["reportDate"].Value,
                                                                        "yyyyMMdd",
                                                                        CultureInfo.InvariantCulture,
                                                                        DateTimeStyles.None);

                                dr["Date"] = theTime;
                                dr["CurrencyID"] = esb.Attributes["currency"].Value;
                                dr["AssetCategoryID"] = esb.Attributes["assetCategory"].Value;
                                dr["Symbol"] = esb.Attributes["symbol"].Value;
                                dr["Description"] = esb.Attributes["description"].Value;
                                dr["PositionValue"] = esb.Attributes["positionValue"].Value;
                                dr["PositionQuantity"] = esb.Attributes["position"].Value;

                                AccountPosition.Rows.Add(dr);
                            }
                        }

                        //Cycle through first each Account Transfer
                        if ((erd.Name == "Transfers") && (advisorAcct == false))
                        {

                            foreach (XmlNode esb in erd.SelectNodes("Transfer"))
                            {
                                DataRow dr;
                                dr = AccountTransfer.NewRow();
                                dr["AccountID"] = esb.Attributes["accountId"].Value;
                                dr["Date"] = esb.Attributes["date"].Value;
                                dr["CurrencyID"] = esb.Attributes["currency"].Value;
                                dr["AssetCategoryID"] = esb.Attributes["assetCategory"].Value;
                                dr["Symbol"] = esb.Attributes["symbol"].Value;
                                dr["Description"] = esb.Attributes["description"].Value;
                                dr["Type"] = esb.Attributes["type"].Value;
                                dr["Direction"] = esb.Attributes["direction"].Value;
                                dr["Account"] = esb.Attributes["account"].Value;
                                dr["Quantity"] = esb.Attributes["quantity"].Value;
                                dr["PositionAmount"] = esb.Attributes["positionAmount"].Value;
                                dr["PositionAmountInBase"] = esb.Attributes["positionAmountInBase"].Value;
                                dr["CashTransfer"] = esb.Attributes["cashTransfer"].Value;

                                string stApp = esb.Attributes["accountId"].Value + esb.Attributes["date"].Value.ToString();

                                if (!acctDateCounterForTransfer.ContainsKey(stApp)) acctDateCounterForTransfer.Add(stApp, 0);
                                else acctDateCounterForTransfer[stApp] = acctDateCounterForTransfer[stApp] + 1;

                                dr["AcctDayCount"] = acctDateCounterForTransfer[stApp];
                                AccountTransfer.Rows.Add(dr);
                            }
                        }


                        //Cycle through first each Account Cash Transaction
                        if ((erd.Name == "CashTransactions") && (advisorAcct == false))
                        {
                            foreach (XmlNode esb in erd.SelectNodes("CashTransaction"))
                            {
                                DataRow dr;
                                dr = AccountCashTransaction.NewRow();
                                dr["AccountID"] = esb.Attributes["accountId"].Value;
                                dr["Date"] = esb.Attributes["dateTime"].Value;
                                dr["Type"] = esb.Attributes["type"].Value;
                                dr["Description"] = esb.Attributes["description"].Value;
                                dr["Amount"] = esb.Attributes["amount"].Value;
                                dr["CurrencyID"] = esb.Attributes["currency"].Value;
                                dr["AssetCategoryID"] = esb.Attributes["assetCategory"].Value;
                                dr["Symbol"] = esb.Attributes["symbol"].Value;
                                dr["TradeID"] = esb.Attributes["tradeID"].Value;

                                AccountCashTransaction.Rows.Add(dr);
                            }
                        }


                        //Cycle through transactions in Statement Funds
                        if ((erd.Name == "StmtFunds") && (advisorAcct == false))
                        {

                            foreach (XmlNode tran in erd.SelectNodes("StatementOfFundsLine"))
                            {
                                //Add the item to the AccountTransaction data table
                                DataRow dt;
                                dt = AccountTransaction.NewRow();
                                //  dt["AccountID"] = tran.Attributes["accountId"].Value;
                                // Let's use the current account ID, since some of these might be bugged and have the Advisor Account instead (gotta love IB)
                                dt["AccountID"] = curAccountId;

                                //Format Date before inserting
                                //                    string format = "mm/dd/yyyy";
                                dt["Date"] = tran.Attributes["reportDate"].Value;

                                //Check if this transaction exists in our DB, if so use the TransactionID, otherwise just ignore that value
                                int appTran = GetTransactionID(TransactionIDMap, tran.Attributes["activityDescription"].Value);
                                if (appTran != -1) dt["TransactionID"] = appTran;
                                //If the transaction can't be found, then use the ID for uncategorized transactions
                                else dt["TransactionID"] = 20;

                                dt["Value"] = tran.Attributes["amount"].Value;
                                dt["TransactionRaw"] = tran.Attributes["activityDescription"].Value;
                                dt["CurrencyID"] = tran.Attributes["currency"].Value;
                                dt["AssetCategoryID"] = tran.Attributes["assetCategory"].Value;
                                dt["Ticker"] = tran.Attributes["symbol"].Value;
                                dt["TradeID"] = tran.Attributes["tradeID"].Value;
                                dt["Balance"] = tran.Attributes["balance"].Value;

                                //    string format = "mm/dd/yyyy";

                                //       string stApp = tran.Attributes["accountId"].Value + Convert.ToDateTime(tran.Attributes["date"].Value).ToShortDateString();
                                string stApp = curAccountId + Convert.ToDateTime(tran.Attributes["reportDate"].Value).ToShortDateString();

                                if (!acctDateCounter.ContainsKey(stApp)) acctDateCounter.Add(stApp, 0);
                                else acctDateCounter[stApp] = acctDateCounter[stApp] + 1;

                                dt["AcctDayCount"] = acctDateCounter[stApp];

                                AccountTransaction.Rows.Add(dt);
                            }
                        }

                        //Cycle through transactions Client Fees
                        if ((erd.Name == "ClientFees") && (advisorAcct == true))
                        {
                            foreach (XmlNode tran in erd.SelectNodes("ClientFee"))
                            {
                                //Add the item to the AccountTransaction data table
                                DataRow dt;
                                dt = AccountTransaction.NewRow();
                                dt["AccountID"] = tran.Attributes["accountId"].Value;

                                //Format Date before inserting
                                //                    string format = "mm/dd/yyyy";
                                dt["Date"] = tran.Attributes["date"].Value;

                                dt["Value"] = tran.Attributes["expense"].Value;
                                dt["TransactionRaw"] = tran.Attributes["description"].Value;
                                dt["CurrencyID"] = tran.Attributes["currency"].Value;
                                dt["TradeID"] = tran.Attributes["tradeID"].Value;

                                //Check if this transaction exists in our DB, if so use the TransactionID, otherwise just ignore that value
                                int appTran = GetTransactionID(TransactionIDMap, tran.Attributes["feeType"].Value);
                                if (appTran != -1)
                                {
                                    dt["TransactionID"] = appTran;
                                    string stApp = tran.Attributes["accountId"].Value + Convert.ToDateTime(tran.Attributes["date"].Value).ToShortDateString();

                                    if (!acctDateCounter.ContainsKey(stApp)) acctDateCounter.Add(stApp, 0);
                                    else acctDateCounter[stApp] = acctDateCounter[stApp] + 1;

                                    dt["AcctDayCount"] = acctDateCounter[stApp];

                                    AccountTransaction.Rows.Add(dt);
                                }
                            }
                        }
                    }
                }
            }

            //Check if DataTables are empty.  If so, throw an error
            if (AccountBalanceDaily.Rows.Count == 0)
            {
                SendMail("Test IBReporting Error! No Data from FlexQuery", "No Data was pulled from the FlexQuery", Emails);
                Environment.Exit(0);
            }

            //printDataTable(AccountBalanceDaily, "AccountBalanceDaily.txt");
            //printDataTable(AccountTransaction, "AccountTransaction.txt");
            //printDataTable(AccountPosition, "AccountPosition.txt");
            //printDataTable(AccountCashTransaction, "AccountCashTransaction.txt");
            //printDataTable(AccountTransfer, "AccountTransfer.txt");

            //Update AccountBalanceDaily, AccountTransaction, AccountPosition, AccountCashTransaction tables on AccountMgmt DB
            UpdateTableOnDB(conn, "usp_InsertAccountBalanceDaily", "@tvp_UpdateAccountBalanceDaily", AccountBalanceDaily);
            UpdateTableOnDB(conn, "usp_InsertAccountTransaction", "@tvp_UpdateAccountTransaction", AccountTransaction);
            UpdateTableOnDB(conn, "usp_InsertAccountPosition", "@tvp_UpdateAccountPosition", AccountPosition);
            UpdateTableOnDB(conn, "usp_InsertAccountTransfer", "@tvp_UpdateAccountTransfer", AccountTransfer);
            // UpdateTableOnDB(conn, "usp_InsertAccountCashTransaction", "@tvp_UpdateAccountCashTransaction", AccountCashTransaction);
        }

        //Procedure to send mail from DaVinci
        static void SendMail(string subject, string errorDescription, string eMailTo)
        {
            System.Net.Mail.MailMessage message = new System.Net.Mail.MailMessage();
            message.To.Add(eMailTo);
            message.Subject = subject;
            message.From = new System.Net.Mail.MailAddress("davinci@appianroad.com");
            message.Body = errorDescription;

            SmtpClient smtp = new SmtpClient("smtp.gmail.com", 587);

            NetworkCredential nc = new NetworkCredential(ConvertToUnsecureString(eMailUser), ConvertToUnsecureString(eMailPwd));
            smtp.Credentials = nc;
            smtp.EnableSsl = true;

            smtp.Send(message);
        }

        static void GrabNProcessAccountsData(string queryName, SecureString flexToken, SecureString sqlLogin, SecureString sqlPwd)
        {
            try
            {
                //Set connection string parameter
                string connectionString = "";

                if (dbUsed == "Test")
                {
                    connectionString = ConfigurationManager.ConnectionStrings["AccountMgmtcon"].ToString();
                }
                else
                {
                    connectionString = "Server = cypy4eiklp.database.windows.net; Database = AccountMgmt; User Id = " + ConvertToUnsecureString(sqlLogin) + "; Password = " + ConvertToUnsecureString(sqlPwd) + ";";
                }
                //Pull flex query ID
                int queryID = -1;
                SqlDataReader rdr = null;
                SqlConnection conn = new SqlConnection(connectionString);
                SqlCommand command = new SqlCommand("GetFlexQueryID", conn);
                command.CommandType = CommandType.StoredProcedure;

                command.Parameters.Add(new SqlParameter("@FlexQueryName", queryName));

                conn.Open();
                rdr = command.ExecuteReader();
                rdr.Read();
                queryID = (int)rdr["ID"];
                conn.Close();

                //Make sure it returned a valid queryID value
                if (queryID != -1)
                {

                    //Construct flex query URL
                    string flexURL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?t=" + ConvertToUnsecureString(flexToken) + "&q=" + queryID.ToString() + "&v=3";

                    //Hit flex web-service to run remotely the flex query
                    XmlDocument doc = new XmlDocument();
                    doc.Load(flexURL);

                    //Grab the reportID
                    XmlElement root = doc.DocumentElement;
                    XmlNode node = root.SelectSingleNode("ReferenceCode");
                    if (node != null)
                    {
                        string referenceCode = node != null ? node.InnerText : "";

                        if (!string.IsNullOrEmpty(referenceCode))
                        {
                            //Construct flex query report URL
                            flexURL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?q=" + referenceCode + "&t=" + ConvertToUnsecureString(flexToken) + "&v=3";

                            //Hit flex web-service to pull the full report
                            XmlDocument docReport = new XmlDocument();

                            int counter = 0;
                            string xmlStr = "";

                            while (true)
                            {
                                if (counter > 49)
                                {
                                    SendMail("Test IBReporting Error!", "No data found at 49 attempt!", Emails);
                                    break;
                                }
                                else
                                {
                                    using (var wc = new WebClient())
                                    {
                                        xmlStr = wc.DownloadString(flexURL);
                                    }
                                    docReport.Load(flexURL);
                                    XmlElement rootdata = docReport.DocumentElement;
                                    XmlNode nodes = rootdata.SelectSingleNode("ErrorCode");
                                    if (nodes != null)
                                    {
                                        counter = counter + 1;
                                        System.Threading.Thread.Sleep(2000);
                                        continue;
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                            if (counter < 49)
                            {
                                //Cycle through each Account and store the data into the AccountMgmt Database
                                ProcessXMLResult(conn, docReport);
                            }
                        }
                        else
                        {
                            //throw e;
                            //send mail to support@appianroad.com specifying error
                            SendMail("Test IBReporting Error!", "referenceCode not found", Emails);
                            Environment.Exit(0);
                        }
                    }
                    else
                    {
                        // no refrence code found
                    }

                }
                //When it's all done - clean the flextoken clear
                flexToken.Clear();
            }
            catch (Exception e)
            {
                //throw e;
                //send mail to support@appianroad.com specifying error
                SendMail("Test IBReporting Error!", e.ToString(), Emails);
                Environment.Exit(0);
            }
        }


        static void VerifyDataUpload(SecureString sqlLogin, SecureString sqlPwd)
        {
            try
            {
                //Set connection string parameter
                string connectionString = "";

                if (dbUsed == "Test")
                {
                    connectionString = ConfigurationManager.ConnectionStrings["AccountMgmtcon"].ToString();
                }
                else
                {
                    connectionString = "Server = cypy4eiklp.database.windows.net; Database = AccountMgmt; User Id = " + ConvertToUnsecureString(sqlLogin) + "; Password = " + ConvertToUnsecureString(sqlPwd) + ";";
                }

                //Pull flex query ID
                string result;
                SqlDataReader rdr = null;
                SqlConnection conn = new SqlConnection(connectionString);
                SqlCommand command = new SqlCommand("VerifyDataUploads", conn);
                command.CommandType = CommandType.StoredProcedure;

                conn.Open();
                rdr = command.ExecuteReader();
                rdr.Read();
                result = (string)rdr["Result"];
                conn.Close();

                //Make sure it didn't return any error, if so - send error eMail
                if (!(result.Contains("-1")))
                {
                    //send mail to support@appianroad.com specifying error
                    SendMail("Test IBReporting Error! No Data in DB", result, Emails);
                    Environment.Exit(0);
                }
            }
            catch (Exception e)
            {
                //throw e;
                //send mail to support@appianroad.com specifying error
                SendMail("Test IBReporting Error!", e.ToString(), Emails);
                Environment.Exit(0);
            }
        }

        static void UpdateIndependentTables(SecureString sqlLogin, SecureString sqlPwd)
        {
            try
            {
                //Set connection string parameter
                string connectionString = "";

                if (dbUsed == "Test")
                {
                    connectionString = ConfigurationManager.ConnectionStrings["AccountMgmtcon"].ToString();
                }
                else
                {
                    connectionString = "Server = cypy4eiklp.database.windows.net; Database = AccountMgmt; User Id = " + ConvertToUnsecureString(sqlLogin) + "; Password = " + ConvertToUnsecureString(sqlPwd) + ";";
                }

                //This inserts new values into AccountRiskProfilesDaily (used to calcualte risk profile returns) - can't do it via Views, too slow.              
                string result;
                SqlConnection conn = new SqlConnection(connectionString);
                SqlCommand command = new SqlCommand("usp_InsertAccountRiskProfileDaily", conn);
                command.CommandType = CommandType.StoredProcedure;

                conn.Open();
                command.ExecuteNonQuery();
                conn.Close();

            }
            catch (Exception e)
            {
                //throw e;
                //send mail to support@appianroad.com specifying error
                SendMail("Test IBReporting Error!", e.ToString(), Emails);
                Environment.Exit(0);
            }
        }



        static void Main(string[] args)
        {
            try
            {
                //Check arguments for Flex Query selection
                if (args == null)
                {
                    Console.WriteLine("args is null"); // Check for null array
                }
                else
                {
                    //Pull Flex Query Name
                    string queryName = args[0];
                    Console.WriteLine("args: " + queryName);

                    //Use secure strings to store temporarily credentials
                    SecureString aesCryptPass = new SecureString();
                    SecureString flexToken = new SecureString();
                    SecureString sqlLogin = new SecureString();
                    SecureString sqlPwd = new SecureString();


                    //To grab credentials data from DropBox and store it locally in the temp folder
                    string dropBoxHome = getDropBoxHome();
                    string tempPath = System.IO.Path.GetTempPath();

                    //To avoid storing the password into a variable (and thus being visible in the clear)
                    "@3$CryptPr0t3ct10n".ToCharArray().ToList().ForEach(p => aesCryptPass.AppendChar(p));

                    //Decrypt Flex Token file and 
                    //Read flex token into secure string
                    SharpAESCrypt.SharpAESCrypt.Decrypt(ConvertToUnsecureString(aesCryptPass), dropBoxHome + "\\DaVinci's corner\\Setup\\Flex Token.txt.aes", tempPath + "\\Flex Token.txt");
                    GetLineFromFile(tempPath + "\\Flex Token.txt", 1).ToCharArray().ToList().ForEach(p => flexToken.AppendChar(p));

                    //Decrypt sqlLogin credentials file and 
                    //Read SQLLogin creds info into secure string(s)
                    SharpAESCrypt.SharpAESCrypt.Decrypt(ConvertToUnsecureString(aesCryptPass), dropBoxHome + "\\DaVinci's corner\\Setup\\Credentials.txt.aes", tempPath + "\\Credentials.txt");
                    GetLineFromFile(tempPath + "\\Credentials.txt", 1).ToCharArray().ToList().ForEach(p => sqlLogin.AppendChar(p));
                    GetLineFromFile(tempPath + "\\Credentials.txt", 2).ToCharArray().ToList().ForEach(p => sqlPwd.AppendChar(p));

                    //Decrypt eMail credentials file and 
                    //Read eMailUser and eMailPwd creds info into secure string(s)
                    SharpAESCrypt.SharpAESCrypt.Decrypt(ConvertToUnsecureString(aesCryptPass), dropBoxHome + "\\DaVinci's corner\\Setup\\eMailCredentials.txt.aes", tempPath + "\\eMailCredentials.txt");
                    GetLineFromFile(tempPath + "\\eMailCredentials.txt", 1).ToCharArray().ToList().ForEach(p => eMailUser.AppendChar(p));
                    GetLineFromFile(tempPath + "\\eMailCredentials.txt", 2).ToCharArray().ToList().ForEach(p => eMailPwd.AppendChar(p));


                    //Run flex query, parse data and store it in AccountMgmt database
                    GrabNProcessAccountsData(queryName, flexToken, sqlLogin, sqlPwd);

                    ////Check if data has been uploaded for this run (Greying this out for now as it keep timing out; 12/3/2014)
                    //VerifyDataUpload(sqlLogin, sqlPwd);

                    //Update Independent Tables (needed after reporting data has been uploaded for the day)
                    UpdateIndependentTables(sqlLogin, sqlPwd);

                    //Clean up decrypted data
                    System.IO.File.Delete(tempPath + "\\Flex Token.txt");
                    System.IO.File.Delete(tempPath + "\\Credentials.txt");
                    System.IO.File.Delete(tempPath + "\\eMailCredentials.txt");

                    //Send mail to make sure it ran
                    SendMail("Test IBReporting Successful run: " + System.DateTime.Now.ToString(), "", Emails);

                    //Clean up secure string with encrypted pass
                    aesCryptPass.Clear();
                    flexToken.Clear();
                    sqlLogin.Clear();
                    sqlPwd.Clear();
                    eMailUser.Clear();
                    eMailPwd.Clear();

                }
            }
            catch (Exception e)
            {
                //throw e;
                //send mail to support@appianroad.com specifying error
                SendMail("Test IBReporting Error!", e.ToString(), Emails);
                Environment.Exit(0);
            }
        }

    }
}
