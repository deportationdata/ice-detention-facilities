#!/bin/bash
set -e

Rscript code/1-process-dedicated-nondedicated.R
Rscript code/1-process-detention-data.R
Rscript code/1-process-detention-management.R
Rscript code/1-process-eoir-data.R
Rscript code/1-process-federal-court-districts.R
Rscript code/1-process-foia-05655.R
Rscript code/1-process-foia-10-2554-527.R
Rscript code/1-process-foia-14-09300.R
Rscript code/1-process-foia-22955.R
Rscript code/1-process-foia-41855.R
Rscript code/1-process-foia-list-2007.R
Rscript code/1-process-foia-list-2015.R
Rscript code/1-process-foia-list-2017.R
Rscript code/1-process-hifld-law-enforcement-facilities.R
Rscript code/1-process-hifld-prisons.R
Rscript code/1-process-hold-rooms.R
Rscript code/1-process-hospitals.R
Rscript code/1-process-ice-website.R 
Rscript code/1-process-icpsr-38323-jails-prisons.R
Rscript code/1-process-manual-edits.R
Rscript code/1-process-marshall.R
Rscript code/1-process-vera.R
Rscript code/2-stack.R
Rscript code/3-name-state-match.R
# Rscript code/4-match-jails-prisons.R
Rscript code/4-match-hospitals.R
Rscript code/4-name-code-match.R
Rscript code/5-pick-attributes-latest.R
# Rscript code/6-facilities-geocode.R
Rscript code/7-augment-dataset.R
Rscript code/8-format-dataset.R