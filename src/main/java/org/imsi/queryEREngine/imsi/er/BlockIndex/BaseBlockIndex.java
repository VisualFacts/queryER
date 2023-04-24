package org.imsi.queryEREngine.imsi.er.BlockIndex;

import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;

public class BaseBlockIndex extends BlockIndex{

	public int createBlockIndex(CsvEnumerator<Object[]> enumerator, Integer key) {
		while(enumerator.moveNext()) {
			Object[] currentLine = enumerator.current();
			Integer fields = currentLine.length;
			if(currentLine[key].toString().equals("")) continue;
			
			EntityProfile eP = new EntityProfile(currentLine[key].toString());
			int index = 0;
			while(index < fields) {
				if(index != key) {
					eP.addAttribute(index, currentLine[index].toString());
				}
				
				index ++;
			}
			this.entityProfiles.add(eP);
		}
		enumerator.close();
		return this.entityProfiles.size();
	}

}
