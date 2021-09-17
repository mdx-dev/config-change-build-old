READ ME


Step 1.) ccb.validate_incentive_import

Validate imported spreadsheet using ccb.validate_incentive_import to determine whether or not the file is ready to be put into ConfigChangeBuildDetail.


Step 2.) ccb.build_detail_insert

Once validated, feed the import table name, ticket name, and desired build name to ccb.build_detail_insert.
This will create a new record in ccb.ConfigChangeBuild table.
Next, the imported file table will be transformed and loaded into ccb.ConfigChangeBuildDetail. The new configs and settings will be stored there alongisde the currently active (if there are any) configs and settings.

Step 3.) ccb.deploy_build

Feed the ID of the build you want to deploy to ccb.deploy_build. This will deploy the settings from ccb.ConfigChangeBuildDetail and update IsDeployed = 1 and DateLastDeployed in ccb.ConfigChangeBuild.


Step 4.) ccb.rollback_build

If you want to rollback an already deployed build to the previous version, feed ccb.rollback_build the build ID of the build you want rolledback. It will first check to see if any of the build settings have been changed, in which case it will not perform the rollback. If the rollback is successful, it will update IsDeployed = 0 and IsRolledback = 1 in ccb.ConfigChangeBuild, along with DateLastRolledBack.
