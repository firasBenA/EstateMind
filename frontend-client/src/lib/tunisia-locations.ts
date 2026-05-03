export interface Delegation {
  Name: string;
  NameAr: string;
  Value: string;
  PostalCode: string;
  Latitude: number;
  Longitude: number;
}

export interface Governorate {
  Name: string;
  NameAr: string;
  Value: string;
  Delegations: Delegation[];
}



import tunisiaData from '../../../data/data/data.ts';

export const governorates: Governorate[] = tunisiaData;

export const getGovernorateCoordinates = (governorateName: string): [number, number] | null => {
  const gov = governorates.find(g => 
    g.Name.toLowerCase() === governorateName.toLowerCase() ||
    g.Value.toLowerCase() === governorateName.toLowerCase()
  );
  
  if (!gov || gov.Delegations.length === 0) return null;
  
  // Return the first delegation's coordinates as the governorate center
  const firstDelegation = gov.Delegations[0];
  return [firstDelegation.Latitude, firstDelegation.Longitude];
};

export const getDelegationCoordinates = (delegationName: string): [number, number] | null => {
  for (const gov of governorates) {
    const del = gov.Delegations.find(d => 
      d.Name.toLowerCase() === delegationName.toLowerCase() ||
      d.Value.toLowerCase() === delegationName.toLowerCase()
    );
    if (del) {
      return [del.Latitude, del.Longitude];
    }
  }
  return null;
};

export const getGovernorateDelegations = (governorateName: string): Delegation[] => {
  const gov = governorates.find(g => 
    g.Name.toLowerCase() === governorateName.toLowerCase() ||
    g.Value.toLowerCase() === governorateName.toLowerCase()
  );
  return gov?.Delegations || [];
};