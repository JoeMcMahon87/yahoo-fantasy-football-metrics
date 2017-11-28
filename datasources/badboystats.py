from bs4 import BeautifulSoup
import requests
import csv


class BadBoyStats(object):

    def __init__(self):
        url = "https://www.usatoday.com/sports/nfl/arrests/"
        r = requests.get(url)
        data = r.text
        soup = BeautifulSoup(data, "html.parser")
        self.badboydata = {}
        self.rankings = {}
        with open('cases.csv', mode='r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile)
            for rows in reader:
                case = rows[0].upper().strip()
                if case.startswith('"') and case.endswith('"'):
                    case = case[1:-1]
                rank = int(rows[1])
                # print("Loaded {}".format(case))
                self.rankings[case] = rank

        for row in soup.findAll('tr'):
            cells = row.findAll('td')
            if len(cells) > 0:
                name = cells[2].text
                team = cells[1].text
                date = cells[0].text
                pos = cells[3].text
                case = cells[4].text.upper()
                tempcat = cells[5].text.upper()

                for item in tempcat.strip().split(","):
                    cat = item.strip()
                    if cat in self.rankings:
                        score = self.rankings.get(cat)
                    else:
                        print("Crime ranking not found: " + cat)

                    if name not in self.badboydata:
                        self.badboydata[name] = {
                            'team': team,
                            'date': date,
                            'pos': pos,
                            'case': case,
                            'cat': cat,
                            'points': score
                        }
                    else:
                        points = self.badboydata[name].get("points") + score
                        self.badboydata[name]['points'] = points

                    if pos in ['CB', 'LB', 'DE', 'DT', 'S']:
                        if team not in self.badboydata:
                            self.badboydata[team] = {
                                'team': team,
                                'date': date,
                                'pos': pos,
                                'case': case,
                                'cat': cat,
                                'points': score
                            }
                        else:
                            points = self.badboydata[name].get("points") + score
                            self.badboydata[name]['points'] = points
        print("{} bad boy records loaded".format(len(self.badboydata)))

    def check_bad_boy_status(self, name, team, pos):
        total = 0
        if pos == 'DEF':
            name = team
        if name in self.badboydata:
            crime = self.badboydata.get(name)
            total = crime.get("points")
        return total
