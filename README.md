# TV-recording-pipeline  <a href="https://athenasowl.tv"><img align="right" src="https://imgur.com/rJzO7hX.png"></a>
<br></br>

Codes for the TV recording pipeline for BARC

## Cron Jobs for Email Notification

- When recording starts for primary or secondary device , email should be sent.
- When recording does not start for primary or secondary device , email should be sent.

```sh

python cronjobs/startVideoSendMail.py
 
```

- When recording retried for 3 times , email should be sent

```sh

python cronjobs/retrySendMail.py
 
```
- On master PC these cron jobs are deployed.
- crontab -e


```sh

40 17 * * * python cronjobs/startVideoSendMail.py >> /tmp/startVideoSendMail.log
* * * * * python cronjobs/retrySendMail.py >> /tmp/retrySendMail.log

```