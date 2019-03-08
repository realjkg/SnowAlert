require('dplyr')
#This expects:
#input_table
#event_day
#pivot


input <- input_table
input$event_day <- as.Date(input$event_day, '%Y-%m-%d', tz="GMT")
return_value <- input %>% group_by(pivot) %>%summarize(num_rows_overall=n(), num_days=length(unique(event_day)))
