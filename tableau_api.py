# -*- coding: utf-8 -*-
"""
@author: Yu Cheng based on jayaharyonomanik
"""


import logging

import pandas as pd
import tableauserverclient as TSC


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s %(message)s")


class TableauApi:
    def __init__(self, username, password, tableau_api_url, tableau_url, site_id):
        self.username = username
        self.password = password
        self.tableau_api_url = tableau_api_url
        self.tableau_url = tableau_url
        self.site_id = site_id

        # Setup Tableau Server
        self.tableau_auth = TSC.TableauAuth(self.username, self.password)
        self.server = TSC.Server(self.tableau_url, use_server_version=True)

    def get_all_projects(self):
        # Get all projects
        with self.server.auth.sign_in(self.tableau_auth):
            all_projects = list(TSC.Pager(self.server.projects))
            project_list = []
            for project in all_projects:
                project_list.append((project.name, project.id, project.parent_id))
            # Create dataframe of relevant attributes
            project_df = pd.DataFrame(
                data=project_list, columns=["ProjectNM", "ProjectID", "ParentProjectID"]
            )
            # Map project to parent project
            project_to_parent = {
                project.id: project.parent_id for project in all_projects
            }
            # Map project to name
            project_to_name = {project.id: project.name for project in all_projects}
        return project_df, project_to_parent, project_to_name

    def list_all_data_sources(self):
        with self.server.auth.sign_in(self.tableau_auth):
            all_datasources, pagination_item = self.server.datasources.get()
            logging.info(
                f"There are { pagination_item.total_available } datasources on site: "
            )

            while len(all_datasources) < pagination_item.total_available:
                request_options = TSC.RequestOptions(
                    pagenumber=pagination_item.page_number + 1
                )
                datasources, pagination_item = self.server.datasources.get(
                    request_options
                )
                all_datasources.extend(datasources)
            return all_datasources

    def list_all_workbooks(self):
        with self.server.auth.sign_in(self.tableau_auth):
            all_workbooks, pagination_item = self.server.workbooks.get()
            logging.info(
                f"There are { pagination_item.total_available } workbooks on site: "
            )

            while len(all_workbooks) < pagination_item.total_available:
                request_options = TSC.RequestOptions(
                    pagenumber=pagination_item.page_number + 1
                )
                workbooks, pagination_item = self.server.workbooks.get(request_options)
                all_workbooks.extend(workbooks)
            return all_workbooks

    def get_workbook_detail(self, workbook_id):
        with self.server.auth.sign_in(self.tableau_auth):
            workbook = self.server.workbooks.get_by_id(workbook_id)
            return workbook

    def delete_workbook(self, workbook_id):
        with self.server.auth.sign_in(self.tableau_auth):
            response = self.server.workbooks.delete(workbook_id)
            return response

    def get_project_id_by_name(self, project_path):
        # If project_path not exist, create or return error
        # return project_id (need for publish)
        # Assume only top levels
        project_id = None
        project_df, _, _ = self.get_all_projects()
        candidates = project_df[project_df["ProjectNM"].str.contains(project_path)]
        if candidates.empty:  # no existing project contains the project_path
            logging.info(f"The project does not exist, no permission to create one.")
        else:
            project_id = candidates[candidates["ProjectNM"] == project_path][
                "ProjectID"
            ].values[0]
        return project_id

    # Still figuring out how to put description in workbook via this api
    def publish_workbook(
        self,
        name,
        project_id,
        file_path,
        hidden_views=None,
        show_tabs=False,
        tags=None,
        description=None,  # Not settable through API as of today
    ):
        with self.server.auth.sign_in(self.tableau_auth):
            new_workbook = TSC.WorkbookItem(
                name=name, project_id=project_id, show_tabs=show_tabs
            )
            if tags is not None:
                new_workbook.tags = set(tags)

            new_workbook = self.server.workbooks.publish(
                new_workbook,
                file_path,
                "Overwrite",
                hidden_views=hidden_views,
                skip_connection_check=True,
            )
        return new_workbook
