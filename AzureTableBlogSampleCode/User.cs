using System;


namespace AzureTableBlogSampleCode
{
    public class User : Microsoft.WindowsAzure.Storage.Table.TableEntity
    {
        public string Name { get; set; }
        public int Salary { get; set; }
        public string Mobile { get; set; }
        public DateTime Birth { get; set; }

    }
}
