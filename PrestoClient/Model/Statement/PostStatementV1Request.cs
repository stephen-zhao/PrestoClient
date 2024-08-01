﻿using System;
using System.Collections.Generic;
using System.Text;

namespace BAMCIS.PrestoClient.Model.Statement
{
    /// <summary>
    /// A new request to POST to the v1 /statement endpoint
    /// </summary>
    public class PostStatementV1Request
    {
        /// <summary>
        /// The query to execute
        /// </summary>
        public string Query { get; }

        /// <summary>
        /// The API version being used for this request.
        /// </summary>
        public StatementApiVersion ApiVersion { get; }

        /// <summary>
        /// Additional options
        /// </summary>
        public QueryOptions Options { get; set; }

        /// <summary>
        /// Creates a new query statement request with the specified query.
        /// </summary>
        /// <param name="query">The query statement to execute.</param>
        public PostStatementV1Request(string query)
        {
            if (String.IsNullOrEmpty(query))
            {
                throw new ArgumentNullException("query", "The query cannot be null or empty.");
            }

            // Trim any trailing semi-colons. Using the REST API, presto doesn't
            // want these and will throw an error if present.
            this.Query = query.TrimEnd(';');
            this.ApiVersion = StatementApiVersion.V1;
        }

        /// <summary>
        /// Creates a new query statement request with the specified query and options.
        /// </summary>
        /// <param name="query">The query statement to execute.</param>
        /// <param name="options">The options to use when executing the statement.</param>
        public PostStatementV1Request(string query, QueryOptions options) : this(query)
        {
            this.Options = options;
        }
    }
}
