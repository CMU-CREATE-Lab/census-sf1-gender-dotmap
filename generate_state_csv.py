import time

from celery import group

from dotgen.tasks import generate_state_csv

YEAR = 2010


STATES = (
 'AK',
 'AL',
 'AR',
 'AZ',
 'CA',
 'CO',
 'CT',
 'DC',
 'DE',
 'FL',
 'GA',
 'HI',
 'IA',
 'ID',
 'IL',
 'IN',
 'KS',
 'KY',
 'LA',
 'MA',
 'MD',
 'ME',
 'MI',
 'MN',
 'MO',
 'MS',
 'MT',
 'NC',
 'ND',
 'NE',
 'NH',
 'NJ',
 'NM',
 'NV',
 'NY',
 'OH',
 'OK',
 'OR',
 'PA',
 'PR',
 'RI',
 'SC',
 'SD',
 'TN',
 'TX',
 'UT',
 'VA',
 'VT',
 'WA',
 'WI',
 'WV',
 'WY',
)


def main():
    # Get the lat long form DB
    tasks = []
    for state in STATES:
        tasks.append(generate_state_csv.s(state, YEAR))
    job = group(tasks)
    result = job.apply_async()
    while result.waiting():
        print "Waiting for all tasks to complete. %d / %d tasks done." % (
                result.completed_count(), len(tasks))
        time.sleep(600)
    print "Done. All tasks sucessful? %s" % result.successful()


if __name__ == '__main__':
    main()

