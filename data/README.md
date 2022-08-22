# Sample dataset

Here is stored the sample dataset used in this hands-on repository.<br/>
The dataset is just a small example representing what kind of data and features 
we work on at Mediaset.

### Columns description

- **video_id** (_String_): The ID of the video watched by the user. This field 
  must not be null.
- **time_spent** (_Integer_): Time spent by the user watching the video 
  (in seconds). This field must not be null.
- **video_duration** (_Integer_): Total duration of the video (in seconds). 
  This field must not be null.
- **customer_id** (_String_): When **user_id** field is empty this field must 
  contains the **device_id**, otherwise this field must contains the **user_id** 
  value. This field must not be null.
- **user_id** (_String_): The unique user identifier. If the user is browsing 
  the website in anonymous mode then this field is empty.
- **device_id** (_String_): The unique device identifier used by the user.
  Each device must have a unique identifier, so this field must not be empty.