using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraBatch
{
    class TestModel
    {
        public const string TimestampFmt = "yyyy-MM-dd HH:mm:ss.fffZ";
        public static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public int id;
        public string name;
        public DateTime csharpDate;
        public DateTime cqlDate;
        public Guid tuuid;
        public long writeMSecs;

        public DateTime tuuidDateTime
        {
            get
            {
                return ((TimeUuid)tuuid).GetDate().UtcDateTime;
            }
        }
        
        public DateTime writeDateTime
        {
            get
            {
                return FromMillisecondsSinceUnixEpoch(writeMSecs);
            }
        }

        public DateTime FromMillisecondsSinceUnixEpoch(long milliseconds)
        {
            double seconds = milliseconds / 1000000.0;

            return UnixEpoch.AddSeconds(seconds);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine();
            sb.AppendLine(string.Format("    id                : {0}", id));
            sb.AppendLine(string.Format("    name              : {0}", name));
            sb.AppendLine(string.Format("    csharpDate        : {0}", csharpDate.ToString(TimestampFmt)));
            sb.AppendLine(string.Format("    cqlDate           : {0}", cqlDate.ToString(TimestampFmt)));
            sb.AppendLine(string.Format("    tuuid             : {0}", tuuid));
            sb.AppendLine(string.Format("    tuuid (Date)      : {0}", tuuidDateTime.ToString(TimestampFmt)));
            sb.AppendLine(string.Format("    writeMSecs        : {0}", writeMSecs));
            sb.AppendLine(string.Format("    writeMSecs (Date) : {0}", writeDateTime.ToString(TimestampFmt)));
            sb.AppendLine(string.Format("    DIFF              : {0}", (writeDateTime - csharpDate)));
            sb.AppendLine();

            return sb.ToString();
        }
    }
}
