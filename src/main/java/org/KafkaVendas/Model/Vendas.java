package org.KafkaVendas.Model;

import java.math.BigDecimal;

public class Vendas {
    public Vendas(Long operacao, Long cliente, Integer quantidadeIngressos, BigDecimal valorTolal) {
        this.operacao = operacao;
        this.cliente = cliente;
        this.quantidadeIngressos = quantidadeIngressos;
        this.valorTolal = valorTolal;

    }

    private Long operacao;
    private Long cliente;
    private Integer quantidadeIngressos;
    private BigDecimal valorTolal;
    private String status;

    public Long getOperacao() {
        return operacao;
    }

    public void setOperacao(Long operacao) {
        this.operacao = operacao;
    }

    public Long getCliente() {
        return cliente;
    }

    public void setCliente(Long cliente) {
        this.cliente = cliente;
    }

    public Integer getQuantidadeIngressos() {
        return quantidadeIngressos;
    }

    public void setQuantidadeIngressos(Integer quantidadeIngressos) {
        this.quantidadeIngressos = quantidadeIngressos;
    }

    public BigDecimal getValorTolal() {
        return valorTolal;
    }

    public void setValorTolal(BigDecimal valorTolal) {
        this.valorTolal = valorTolal;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Vendas{" +
                "operacao=" + operacao +
                ", cliente=" + cliente +
                ", quantidadeIngressos=" + quantidadeIngressos +
                ", valorTolal=" + valorTolal +
                ", status='" + status + '\'' +
                '}';
    }
}
