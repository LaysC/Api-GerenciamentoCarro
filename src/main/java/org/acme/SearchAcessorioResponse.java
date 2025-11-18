package org.acme;

import java.util.ArrayList;
import java.util.List;

public class SearchAcessorioResponse {
    public List<Acessorio> Acessorios = new ArrayList<>();

    public long TotalAcessorios;
    public int TotalPages;
    public boolean HasMore;
    public String NextPage;
}