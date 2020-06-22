import os
from datetime import datetime, date
import logging

import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
API_NAME = "analyticsreporting"
API_VERSION = "v4"

logger = logging.getLogger(__name__)


class GAClient:
    """
    Client Instance.
    """

    def __init__(self, json_keyfile=None, view_id=None, start_date=None, end_date=None):
        """
        Read service key file and initialize the API client
        """
        json_keyfile = json_keyfile or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_keyfile, scopes=SCOPES
        )
        self.client = build(API_NAME, API_VERSION, credentials=self.credentials)
        self.set_view_id(view_id)
        self.set_dateranges(start_date, end_date)

        logger.debug("gaapi4py client initiated")

    def _generate_request_body(self, params):
        """
        Read request parameters in simplified form and generate
        proper request body for batch request
        """

        start_date = params.get("start_date", self.start_date)
        end_date = params.get("end_date", self.end_date)
        include_empty_rows = params.get("include_empty_rows", True)

        if isinstance(start_date, date):
            start_date = datetime.strftime(start_date, "%Y-%m-%d")

        if isinstance(end_date, date):
            end_date = datetime.strftime(end_date, "%Y-%m-%d")

        dimensions = list(map(lambda x: {"name": x}, params.get("dimensions")))

        metrics = list(map(lambda x: {"expression": x}, params.get("metrics")))

        segments = []
        if params.get("segments"):
            dimensions.append({"name": "ga:segment"})
            segments.append({"segmentId": params.get("segments")})

        request_body = {
            "viewId": params.get("view_id", self.view_id),
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": dimensions,
            "metrics": metrics, 
            "samplingLevel": "LARGE",
            "includeEmptyRows": str(include_empty_rows),
            "segments": segments,

        }

        if (params.get("filter")):
            request_body["filtersExpression"] = params.get("filter")
        if (params.get("pageToken")):
            request_body["pageToken"] = params.get("pageToken")

        request_body['pageSize'] = '100000'
        
        return request_body

    def _parse_response(self, response_obj):
        """
        Parses and prints the Analytics Reporting API V4 response
        """

        report = response_obj.get("reports", None)[0]
        report_data = report.get("data", {})

        result = {}
        data = []
        header_row = []
        totals_metric_header = []
        metric_type = {}

        column_header = report.get("columnHeader", {})
        metric_header = column_header.get("metricHeader", {}).get(
            "metricHeaderEntries", []
        )
        dimension_header = column_header.get("dimensions", [])

        for dheader in dimension_header:
            header_row.append(dheader)
        for mheader in metric_header:
            metric_name  = mheader["name"].replace("ga:", "")
            header_row.append(metric_name)
            totals_metric_header.append(metric_name)
            metric_type[metric_name] = mheader["type"]

        rows = report_data.get("rows", [])
        for row in rows:
            row_temp = []
            dimensions = row.get("dimensions", [])
            metrics = row.get("metrics", [])
            for d in dimensions:
                row_temp.append(d)
            for m in metrics[0]["values"]:
                row_temp.append(m)
            data.append(row_temp)

        result_df = pd.DataFrame(data, columns=header_row)
        result_df.columns = result_df.columns.str.replace("ga:", "")

        # Calculate Totals row to another DF
        totals = report_data.get("totals",[])
        total = []
        for m in totals[0]['values']:
                total.append(m)

        total_df = pd.DataFrame([total], columns=totals_metric_header)

        if not report_data.get("samplesReadCounts"):
            samples_read_counts = None
            sampling_space_sizes = None
        else:
            samples_read_counts = int(report_data.get("samplesReadCounts", [])[0])
            sampling_space_sizes = int(report_data.get("samplingSpaceSizes", [])[0])
        result["info"] = {
            "isDataGolden": report["data"].get("isDataGolden", False),
            "nextPageToken": report.get("nextPageToken"),
            "samplesReadCounts": samples_read_counts,
            "samplingSpaceSizes": sampling_space_sizes,
        }
        result["data"] = result_df
        result["total"] = total_df
        result["metrics_type"] = metric_type

        return result
    def ga_types_to_df(self, df, dict_types):
        types = {
            'PERCENT':'float',
            'TIME':'float',
            'INTEGER':'int',
            'FLOAT':'float',
            'CURRENCY':'float'
        }
        for col  in df.columns:
            if col in dict_types.keys():
                df[col]=df[col].astype(types[dict_types[col]])
        return df
    def get_all_data(self, params, typed=False):
        """
        Make a single request to GA API with specified parameters
        """
        all_data = []
        response = {}
        while True:
            request_body = self._generate_request_body(params)
            raw_response = (
                self.client.reports()
                .batchGet(body={"reportRequests": request_body})
                .execute()
            )
            parsed = self._parse_response(raw_response)
            all_data.append(parsed["data"])
            params["pageToken"] = parsed.get("info", {}).get("nextPageToken", None)
            if not params["pageToken"]:
                response["info"] = parsed.get("info")
                break
        response["metrics_type"] = parsed.get("metrics_type")

        if typed:
            response["data"] = self.ga_types_to_df(pd.concat(all_data).reset_index(drop=True),response["metrics_type"])
            response["total"] = self.ga_types_to_df(parsed.get("total"),response["metrics_type"])
        else:
            response["data"] = pd.concat(all_data).reset_index(drop=True)
            response["total"] = parsed.get("total")
        

        if not response["info"]["isDataGolden"]:
            logger.warning("Data is not golden")
        if response["info"]["samplesReadCounts"]:
            counts = response["info"]["samplesReadCounts"]
            total = response["info"]["samplingSpaceSizes"]
            logger.warning(
                "Data is sampled! Sampling size: {:.2f}%, {} rows were taken out of {}".format(
                    counts / total * 100, counts, total
                )
            )
        return response

    def set_view_id(self, view_id):
        """
        Specify View ID and use it in all requests, if not explicitly specified
        """
        self.view_id = view_id

    def set_dateranges(self, start_date, end_date):
        """
        Sets default dates to generate request bodies

        Args:
            start_date: string in format 'YYYY-MM-DD'
            end_date: string in format 'YYYY-MM-DD'
        """
        if isinstance(start_date, str):
            self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            self.start_date = start_date
        if isinstance(end_date, str):
            self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        else:
            self.end_date = end_date

    def get_credentials(self):
        """
        Return OAuth2 credentials which are generated from service account
        """
        return self.credentials
