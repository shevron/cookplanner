Cookplanner
===========
Cookplanner is a simple recurring daily task / owner scheduler. It was designed to 
solve a very specific problem:

 - There is a recurring task that needs to be handled daily (or, up to once per day)
 - There is a group of people who should handle that task based on some fair, predictable scheduling
 - These people have constraints and preferences
 - There could also be external constraints such as holidays and weekends in which the task 
   doesn't need to be performed. 

Given these assumptions, Cookplanner will build a schedule for performing the task in a given
date range (e.g. for the next year), and will manage an online Google Calendar describing this schedule.

## Installation
You need Python 3.8 or newer, and `poetry`. 

* TODO: installing

* TODO: configuration

## Why?
Cookplanner was written to manage the cooking schedule of parents in the communal kindergarten where 
I live. Each day, a different parent has to cook, and the preference is to stick to more or less constant
weekdays for each parent (this makes things easier to remember and people get familiarized with the menu, 
which is set per weekday). Some parents have constraints. Some parents have more than one child, and thus
are required to cook more. There are public holidays in which there is no cooking or Kindergarten ends 
early. 

It was only natural to write some Python code and get all of that automated.
