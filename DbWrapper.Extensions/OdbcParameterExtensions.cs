using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DbWrapper.Extensions {

	public static class OdbcParameterExtensions {

		public static string TrimParameter(this OdbcParameter param) {
			Match match = Regex.Match(
				param.ParameterName,
				@"(^@)(\w+.*)([U|W\I]{1}[0-9]*)$",
				RegexOptions.None
			);

			return match.Groups[2].Value;
		}

		public static string RemoveIllegalCharacters(this OdbcParameter param) {
			return Regex.Replace(param.ParameterName, @"\.", "");
		}

	}

}