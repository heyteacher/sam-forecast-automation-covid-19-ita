AWS SAM template for AWS Forecast automation 
============================================

[![Liberpay](http://img.shields.io/liberapay/receives/heyteacher.svg?logo=liberapay)](https://liberapay.com/heyteacher/donate)
[![GitHub license](https://img.shields.io/github/license/heyteacher/sam-forecast-automation-covid-19-ita)](https://github.com/heyteacher/sam-forecast-automation-covid-19-ita/blob/master/LICENSE)
[![GitHub commit](https://img.shields.io/github/last-commit/heyteacher/sam-forecast-automation-covid-19-ita)](https://github.com/heyteacher/sam-forecast-automation-covid-19-ita/commits/master)

A `AWS SAM template` for `AWS Forecast` process automation using `AWS Step Functions` state machine, based on a real case study: Forecast of new daily positive based on COVID-19 italian datasets.  

This `AWS SAM template` is running in my `AWS Account` and push daily the forecast in this repository: https://github.com/heyteacher/COVID-19 (folder `dati_json_forecast`)

Furtermore datasets and forecasts are visualized by this charts dashboard https://heyteacher.github.io/COVID-19 an `Angular 9` project hosted in this repository https://github.com/heyteacher/ng-covid-19-ita-charts 

This `AWS SAM template` is general purpose, so can be adapted to other forecast based on __AWS Forecast__ removing or replacing specific use case tasks. 

It's difficult automate `AWS Forecast` process because:

1. `AWS Forecast` tasks are long running proccess and cannot be start until previous step is succesfully finish

1. `AWS Forecast` doesn't implements push notification (for example via `AWS SNS`) to inform the end of a task, so it isn't possible do create e event driven flow of `AWS Forecast` tasks. It's only possible to poll entity status after creation in order to understand if it's succesfully created.

Why automate `AWS Forecast` task using `AWS Step Functions`? 

Because `AWS Step Functions` is a __Serverless State Machine__ which orchestrate `AWS Lambda` implements `AWS Forecast` api calls managing `AWS Forecast` entities, and support __Retry__, __Fallback__ and other flow controls.

Only the first state machine execution creates the persistent entities __Dataset__, __Dataset Group__ and __Predictor__, while during daily next executions, the forecast will update creating __Forecast Dataset Import Job__, __Forecast__ and __Export Job__

The `AWS Step Functions` is launched by a `AWS Cloud Watch Event Rule` which start following the rule expression defined into `StateMachineEventRuleScheduleExpression` parameter. But the forecast is generated only in day of week defined into `ForecastDaysOfWeekExecution` parameter. 

![AWS Step Functions Forecast](/images/stepfunctions_graph.png "AWS Step Functions Forecast")

Below the daily flow of `AWS Step Functions` steps:

1. `Extend Dataset` is a specific task of case study, you can drop it. Download from daily official dataset, extend it and push in configured Github repository. It retries until a new dataset is pushed into official repository  

1. `CheckDaysOfWeekForecastExec` is a simple inline lambda which set `isToExecuteForecast` = true if the day of week of today is in `ForecastDaysOfWeekExecution` parameter

1. `ChoiceForecastExecution` is a choice on `isToExecuteForecast`: if `true` generate forecast otherwise go to `Done` task and exit

1. `CheckDatasetExist` is the start state, check if the __Dataset__ and (__Dataset Group__) exists. 

   * If it doesn't exist means this is the first execution. `CreateDatase` create the __Dataset__ and __Dataset Group__

1. `WaitGithubRawRefresh` another specific task of case study which can be dopped. It wait some minute in order to be sure the github raw cache is refreshed after push

1. `CreateDatasetImportJob` downloads from configured Github the dataset (new daily COVID-19 time series), trasform the data in order to match che Forecast dataset structure, upload into the __S3 Input Bucket__ and create the daily __Dataset Import Job__

1. `CheckPredictorExists` checks if the predictor exists.

   * If predictor doesn't exist (means this is the firt execution) run `CreatePredictor` which create the __Predictor__. It will be create if there is at least one Dataset Import Job loaded. Then run `WaitPredictorCreation` wait 50 minutes in order to be sure of Predictor creation 

   * otherwise run `WaitDatasetImport` which sleep 5 minutes

1. `CreateForecast` creates the daily __Forecast__ based on __Predictor__ updated by daily __Dataset Import Job__

1. `WaitForecastCreation` sleeps 15 minutes in order be sure of forecast creation is finished

1. `CreateForecastExportJob` exports the daily forecast in `S3 Output Bucket`. 
The upload wake up `PushForecastInGithubFunction` which download the forecast, ad push into configured Github repository (this `AWS Lambda` is  specific of study case)
v
1. `WaitExportJob` sleeps 3 minutes in order to be sure of export is finished

1. `DeleteDatasetImportExportJob` delete the daily __Dataset Import Job__ and the daily __Export Job__ 

1. `WaitDeleteDatasetImportExportJob` sleep 5 minutes in order to be sure of deletion is finished

1. `DeleteForecast` deletes the daily __Forecast__

1. `Done` the end state of workflow

Some tasks retries after a failure in order to wait that previous step is succesfully finished.

The `AWS SAM Template` assign the minimum permission to each `AWS Lambda Functions` in order to complete his task. All the entities (`S3 Bucket`, `AWS Lambda Function`, `IAM Roles`, `AWS Step Functions`, `Event Rule`) are created/updated/deleted by `AWS SAM Template` stack, so no manual activies is needes.


Note
----

* this project is ispired by https://github.com/aws-samples/amazon-automated-forecast

* __BE CAREFULL__ if yoy try to create a stack from this `SAM Template`. First execuction costs 4,00 EUR circa and next daily execution costs 1,00 EUR circa. 

* I already run a stack in my `AWS Account` which produces forecast here https://github.com/heyteacher/COVID-19. So you can support this project making a donation [![Liberpay](https://liberapay.com/assets/widgets/donate.svg)](https://liberapay.com/heyteacher/donate)

* Only `AWS Forecast` entities `Predictor`, the first `Dataset Import Job`, `Dataset` and `Dataset Group` must be deleted manually if you decide to delete `AWS SAM Template` stack.

* All `AWS Lambda` are implemented in `NodeJs 12.X` 

* `AWS Forecast` doesn't implement epidemiological forecasting scenario like COVID-19 Italian new cases series, so the algorithm is choosen by __PerformAutoML=True__. I'm not an expert, so help is appreciated in algorithm tuning for these use case https://docs.aws.amazon.com/forecast/index.html

* I spent a lot of time to improve the `AWS SAM Template` but I'm sure it could be better. So do not esitate so submit [Issue](https://github.com/heyteacher/sam-forecast-automation-covid-19-ita/issues ) or [Pull Request](https://github.com/heyteacher/sam-forecast-automation-covid-19-ita/pulls)

Install
-------

1. install `nodejs` `aws-cli` `aws-sam-cli` `docker`

1. generare `aws_ac-cess_key_id` and `aws_secret_access_key` from a AWS user with the permissions for create/update/delete CloudFormation stacks 

1. create the github repository `<GITHUB_REPO>` in your account `<GITHUB_USER>` 

1. generate a `<GITHUB_TOKEN>` in https://github.com/settings/tokens with scope `repo`

1. to test locally lambda functions (for example `ExtendDataFunction`)
   ```
   sam local invoke ExtendDataFunction \
   --parameter-overrides  GitHubToken=<GITHUB_TOKEN> GitHubRepo=<GITHUB_REPO> GitHubUser=<GITHUB_USER> 
   ```
   Useful bash scripts `sam_local_invoke.sh.template` and `sam_local_invoke_push_github.sh` can be customized in order to run locally lambda functions

Packaging e Deploying
---------------------

Useful bash script `deploy_stack.sh.template` can be customized in order to automate stack deploy (steps `package` and `deploy`)

1. delete old stack 
   ```   
   aws cloudformation delete-stack --stack-name forecast-automation-covid-19-ita
   ```   

1. package
   ```   
   aws cloudformation package --template-file template.yaml \
   --output-template-file packaged.yaml \
   --s3-bucket <SAM_TEMPLATE_BUCKET>
   ```   

1. deploy
   ```   
   aws cloudformation deploy --template-file packaged.yaml  \
   --stack-name forecast-automation-covid-19-ita \
   --capabilities CAPABILITY_IAM \
   --parameter-overrides  GitHubToken=<GITHUB_TOKEN> GitHubRepo=<GITHUB_REPO> GitHubUser=<GITHUB_USER> 
   ```   

1. show stack events
   ```   
   aws cloudformation describe-stack-events --stack-name forecast-automation-covid-19-ita
   ```   

1. tail lambda logs (for example ExtendDataFunction)
   ```   
   sam logs -n ExtendDataFunction --stack-name forecast-automation-covid-19-ita --tail
 
   ```   
